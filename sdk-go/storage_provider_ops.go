package workspace

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/OpenElevo/ElevoWorkspace/sdk-go/proto/workspace/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// executeOperation dispatches a storage operation request to the appropriate handler.
// Returns nil for paginated ListDir operations (they send responses directly via responseCh).
func (sp *StorageProvider) executeOperation(req *pb.StorageOperationRequest) *pb.ClientMessage {
	var result *pb.StorageOperationResponse

	switch op := req.Operation.(type) {
	case *pb.StorageOperationRequest_Stat:
		result = sp.opStat(req.CorrelationId, op.Stat)
	case *pb.StorageOperationRequest_ListDir:
		result = sp.opListDir(req.CorrelationId, op.ListDir)
	case *pb.StorageOperationRequest_Exists:
		result = sp.opExists(req.CorrelationId, op.Exists)
	case *pb.StorageOperationRequest_ReadFileRange:
		result = sp.opReadFileRange(req.CorrelationId, op.ReadFileRange)
	case *pb.StorageOperationRequest_WriteFileAt:
		result = sp.opWriteFileAt(req.CorrelationId, op.WriteFileAt)
	case *pb.StorageOperationRequest_CreateFile:
		result = sp.opCreateFile(req.CorrelationId, op.CreateFile)
	case *pb.StorageOperationRequest_Mkdir:
		result = sp.opMkdir(req.CorrelationId, op.Mkdir)
	case *pb.StorageOperationRequest_RemoveFile:
		result = sp.opRemoveFile(req.CorrelationId, op.RemoveFile)
	case *pb.StorageOperationRequest_RemoveDir:
		result = sp.opRemoveDir(req.CorrelationId, op.RemoveDir)
	case *pb.StorageOperationRequest_Rename:
		result = sp.opRename(req.CorrelationId, op.Rename)
	case *pb.StorageOperationRequest_Copy:
		result = sp.opCopy(req.CorrelationId, op.Copy)
	case *pb.StorageOperationRequest_SetFileSize:
		result = sp.opSetFileSize(req.CorrelationId, op.SetFileSize)
	case *pb.StorageOperationRequest_SetPermissions:
		result = sp.opSetPermissions(req.CorrelationId, op.SetPermissions)
	case *pb.StorageOperationRequest_SetTimes:
		result = sp.opSetTimes(req.CorrelationId, op.SetTimes)
	case *pb.StorageOperationRequest_Symlink:
		result = sp.opSymlink(req.CorrelationId, op.Symlink)
	case *pb.StorageOperationRequest_ReadLink:
		result = sp.opReadLink(req.CorrelationId, op.ReadLink)
	case *pb.StorageOperationRequest_StatFs:
		result = sp.opStatFs(req.CorrelationId)
	default:
		result = errorResponse(req.CorrelationId,
			pb.StorageErrorCode_STORAGE_ERROR_CODE_NOT_SUPPORTED, "unknown operation")
	}

	if result == nil {
		return nil // paginated ListDir sent via responseCh
	}

	return &pb.ClientMessage{
		Message: &pb.ClientMessage_OperationResponse{
			OperationResponse: result,
		},
	}
}

// ── Stat ──

func (sp *StorageProvider) opStat(corrID string, req *pb.StatRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	var stat unix.Stat_t
	if err := unix.Fstatat(dirFd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return osErrorResponse(corrID, err)
	}

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_Stat{
			Stat: statToProto(req.Path, name, &stat),
		},
	})
}

// ── ListDir ──

const listDirPageSize = 200

func (sp *StorageProvider) opListDir(corrID string, req *pb.ListDirRequest) *pb.StorageOperationResponse {
	// Open a fresh directory fd for reading. We must not reuse rootFd directly
	// because Dup shares the underlying open file description (including the
	// directory seek position). After the first ReadDir(-1), subsequent calls
	// on dup'd fds would read from the end of the directory stream, returning
	// 0 entries.
	var dirFd int
	if req.Path == "" || req.Path == "." {
		fd, err := unix.Openat(sp.pathGuard.rootFd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return osErrorResponse(corrID, err)
		}
		dirFd = fd
		defer unix.Close(fd)
	} else {
		parentFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
		if err != nil {
			return pathErrorResponse(corrID, err)
		}
		fd, err := unix.Openat(parentFd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		closeFdIfNotRoot(parentFd, sp.pathGuard.rootFd)
		if err != nil {
			return osErrorResponse(corrID, err)
		}
		dirFd = fd
		defer unix.Close(fd)
	}

	// Read directory entries. dirFd is always a fresh fd owned by this call,
	// so we dup it for os.File to avoid double-close with the deferred Close above.
	dupFd, err := unix.Dup(dirFd)
	if err != nil {
		return osErrorResponse(corrID, err)
	}
	f := os.NewFile(uintptr(dupFd), req.Path)
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return osErrorResponse(corrID, err)
	}

	// Stat each entry relative to the target directory fd.
	statEntries := make([]*pb.FileStatData, 0, len(entries))
	for _, entry := range entries {
		var stat unix.Stat_t
		if err := unix.Fstatat(dirFd, entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			continue // skip entries we can't stat
		}
		entryPath := entry.Name()
		if req.Path != "" && req.Path != "." {
			entryPath = req.Path + "/" + entry.Name()
		}
		statEntries = append(statEntries, statToProto(entryPath, entry.Name(), &stat))
	}

	// Small directory: single response.
	if len(statEntries) <= listDirPageSize {
		return successResponse(corrID, &pb.StorageOperationSuccess{
			Data:   &pb.StorageOperationSuccess_ListDir{ListDir: &pb.ListDirData{Entries: statEntries}},
			IsLast: true,
		})
	}

	// Large directory: paginated responses via responseCh.
	for i := 0; i < len(statEntries); i += listDirPageSize {
		end := i + listDirPageSize
		if end > len(statEntries) {
			end = len(statEntries)
		}
		isLast := end >= len(statEntries)

		sp.trySendResponse(&pb.ClientMessage{
			Message: &pb.ClientMessage_OperationResponse{
				OperationResponse: &pb.StorageOperationResponse{
					CorrelationId: corrID,
					Result: &pb.StorageOperationResponse_Success{
						Success: &pb.StorageOperationSuccess{
							Data:   &pb.StorageOperationSuccess_ListDir{ListDir: &pb.ListDirData{Entries: statEntries[i:end]}},
							IsLast: isLast,
						},
					},
				},
			},
		})
	}

	return nil // paginated mode: already sent
}

// ── Exists ──

func (sp *StorageProvider) opExists(corrID string, req *pb.ExistsRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	var stat unix.Stat_t
	exists := unix.Fstatat(dirFd, name, &stat, unix.AT_SYMLINK_NOFOLLOW) == nil

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_Exists{
			Exists: &pb.ExistsData{Exists: exists},
		},
	})
}

// ── ReadFileRange ──

func (sp *StorageProvider) opReadFileRange(corrID string, req *pb.ReadFileRangeRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	fd, err := unix.Openat(dirFd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return osErrorResponse(corrID, err)
	}

	// length=0 means "read entire file": wrap as os.File and use ReadAll.
	if req.Length == 0 {
		f := fdToFile(fd, req.Path)
		defer f.Close()

		if req.Offset > 0 {
			if _, err := f.Seek(int64(req.Offset), io.SeekStart); err != nil {
				return osErrorResponse(corrID, err)
			}
		}
		data, err := io.ReadAll(f)
		if err != nil {
			return osErrorResponse(corrID, err)
		}
		return successResponse(corrID, &pb.StorageOperationSuccess{
			Data: &pb.StorageOperationSuccess_ReadData{
				ReadData: &pb.ReadData{Data: data},
			},
		})
	}

	// Bounded read with explicit length.
	defer unix.Close(fd)
	buf := make([]byte, req.Length)
	n, err := unix.Pread(fd, buf, int64(req.Offset))
	if err != nil && err != io.EOF {
		return osErrorResponse(corrID, err)
	}

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_ReadData{
			ReadData: &pb.ReadData{Data: buf[:n]},
		},
	})
}

// ── WriteFileAt ──

func (sp *StorageProvider) opWriteFileAt(corrID string, req *pb.WriteFileAtRequest) *pb.StorageOperationResponse {
	lock := sp.acquireFileLock(req.Path)
	if lock == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout")
	}
	defer sp.releaseFileLock(lock)

	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	flags := unix.O_WRONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if req.Offset == 0 {
		// Full-file write: create if missing, truncate existing content
		flags |= unix.O_CREAT | unix.O_TRUNC
	}
	fd, err := unix.Openat(dirFd, name, flags, 0644)
	if err != nil {
		return osErrorResponse(corrID, err)
	}
	defer unix.Close(fd)

	n, err := unix.Pwrite(fd, req.Data, int64(req.Offset))
	if err != nil {
		return osErrorResponse(corrID, err)
	}

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_WriteData{
			WriteData: &pb.WriteData{BytesWritten: uint64(n)},
		},
	})
}

// ── CreateFile ──

func (sp *StorageProvider) opCreateFile(corrID string, req *pb.CreateFileRequest) *pb.StorageOperationResponse {
	lock := sp.acquireFileLock(req.Path)
	if lock == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout")
	}
	defer sp.releaseFileLock(lock)

	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	flags := unix.O_WRONLY | unix.O_CREAT | unix.O_NOFOLLOW | unix.O_CLOEXEC
	if req.Exclusive {
		flags |= unix.O_EXCL
	} else {
		flags |= unix.O_TRUNC
	}

	fd, err := unix.Openat(dirFd, name, flags, 0644)
	if err != nil {
		return osErrorResponse(corrID, err)
	}
	unix.Close(fd)

	return emptySuccessResponse(corrID)
}

// ── Mkdir ──

func (sp *StorageProvider) opMkdir(corrID string, req *pb.StorageMkdirRequest) *pb.StorageOperationResponse {
	if req.Recursive {
		return sp.opMkdirRecursive(corrID, req.Path)
	}

	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	if err := unix.Mkdirat(dirFd, name, 0755); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

func (sp *StorageProvider) opMkdirRecursive(corrID string, path string) *pb.StorageOperationResponse {
	if err := sp.pathGuard.ValidatePath(path); err != nil {
		return pathErrorResponse(corrID, err)
	}

	// Walk each component using Mkdirat for fd-based TOCTOU safety.
	cleaned := filepath.Clean(path)
	parts := strings.Split(cleaned, "/")

	currentFd := sp.pathGuard.rootFd
	for _, component := range parts {
		if component == "" || component == "." {
			continue
		}

		// Try to create the directory; EEXIST is fine (already exists).
		err := unix.Mkdirat(currentFd, component, 0755)
		if err != nil && err != unix.EEXIST {
			if currentFd != sp.pathGuard.rootFd {
				unix.Close(currentFd)
			}
			return osErrorResponse(corrID, err)
		}

		// Open the (possibly pre-existing) directory for the next iteration.
		nextFd, err := unix.Openat(currentFd, component,
			unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if currentFd != sp.pathGuard.rootFd {
			unix.Close(currentFd)
		}
		if err != nil {
			return osErrorResponse(corrID, err)
		}
		currentFd = nextFd
	}

	if currentFd != sp.pathGuard.rootFd {
		unix.Close(currentFd)
	}
	return emptySuccessResponse(corrID)
}

// ── RemoveFile ──

func (sp *StorageProvider) opRemoveFile(corrID string, req *pb.RemoveFileRequest) *pb.StorageOperationResponse {
	lock := sp.acquireFileLock(req.Path)
	if lock == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout")
	}
	defer sp.releaseFileLock(lock)

	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	if err := unix.Unlinkat(dirFd, name, 0); err != nil {
		return osErrorResponse(corrID, err)
	}

	// Clean up the file lock entry.
	sp.fileLocks.Delete(req.Path)

	return emptySuccessResponse(corrID)
}

// ── RemoveDir ──

func (sp *StorageProvider) opRemoveDir(corrID string, req *pb.RemoveDirRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	if req.Recursive {
		// Open the target directory fd for recursive deletion.
		targetFd, err := unix.Openat(dirFd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			return osErrorResponse(corrID, err)
		}
		if err := sp.removeAllAt(targetFd); err != nil {
			unix.Close(targetFd)
			return osErrorResponse(corrID, err)
		}
		unix.Close(targetFd)
	}

	// Remove the (now empty) directory.
	if err := unix.Unlinkat(dirFd, name, unix.AT_REMOVEDIR); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

// removeAllAt recursively deletes all contents of a directory identified by fd.
// Uses fd-based operations to avoid TOCTOU races.
func (sp *StorageProvider) removeAllAt(dirFd int) error {
	// Dup the fd so os.File.Close() doesn't close the caller's fd.
	dupFd, err := unix.Dup(dirFd)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(dupFd), "")
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			childFd, err := unix.Openat(dirFd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			if err != nil {
				return err
			}
			if err := sp.removeAllAt(childFd); err != nil {
				unix.Close(childFd)
				return err
			}
			unix.Close(childFd)
			if err := unix.Unlinkat(dirFd, name, unix.AT_REMOVEDIR); err != nil {
				return err
			}
		} else {
			if err := unix.Unlinkat(dirFd, name, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

// ── Rename ──

func (sp *StorageProvider) opRename(corrID string, req *pb.RenameRequest) *pb.StorageOperationResponse {
	// Acquire locks in lexicographic order to prevent deadlocks.
	path1, path2 := req.Src, req.Dst
	if path1 > path2 {
		path1, path2 = path2, path1
	}
	lock1 := sp.acquireFileLock(path1)
	if lock1 == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout (src)")
	}
	defer sp.releaseFileLock(lock1)

	lock2 := sp.acquireFileLock(path2)
	if lock2 == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout (dst)")
	}
	defer sp.releaseFileLock(lock2)

	srcDirFd, srcName, err := sp.pathGuard.OpenParentDir(req.Src)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(srcDirFd, sp.pathGuard.rootFd)

	dstDirFd, dstName, err := sp.pathGuard.OpenParentDir(req.Dst)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dstDirFd, sp.pathGuard.rootFd)

	var flags uint
	switch req.Flags {
	case 1: // NOREPLACE
		flags = platformRenameNoreplace()
	case 2: // EXCHANGE
		flags = platformRenameExchange()
	default:
		flags = 0
	}

	if err := platformRenameat2(srcDirFd, srcName, dstDirFd, dstName, flags); err != nil {
		return osErrorResponse(corrID, err)
	}

	// Clean up old path's lock entry.
	sp.fileLocks.Delete(req.Src)

	return emptySuccessResponse(corrID)
}

// ── Copy ──

func (sp *StorageProvider) opCopy(corrID string, req *pb.CopyRequest) *pb.StorageOperationResponse {
	// Open source via fd-based path traversal.
	srcDirFd, srcName, err := sp.pathGuard.OpenParentDir(req.Src)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(srcDirFd, sp.pathGuard.rootFd)

	// Open destination parent via fd-based path traversal.
	dstDirFd, dstName, err := sp.pathGuard.OpenParentDir(req.Dst)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dstDirFd, sp.pathGuard.rootFd)

	// Stat the source to determine if it's a file or directory.
	var srcStat unix.Stat_t
	if err := unix.Fstatat(srcDirFd, srcName, &srcStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return osErrorResponse(corrID, err)
	}

	if platformStatMode(&srcStat)&unix.S_IFMT == unix.S_IFDIR {
		if err := copyDirAt(srcDirFd, srcName, dstDirFd, dstName); err != nil {
			return osErrorResponse(corrID, err)
		}
	} else {
		if err := copyFileAt(srcDirFd, srcName, dstDirFd, dstName, platformStatMode(&srcStat)&0o7777); err != nil {
			return osErrorResponse(corrID, err)
		}
	}

	return emptySuccessResponse(corrID)
}

// copyFileAt copies a single file using fd-based operations (O_NOFOLLOW).
func copyFileAt(srcDirFd int, srcName string, dstDirFd int, dstName string, mode uint32) error {
	srcFd, err := unix.Openat(srcDirFd, srcName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	sf := fdToFile(srcFd, srcName)
	defer sf.Close()

	dstFd, err := unix.Openat(dstDirFd, dstName,
		unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_NOFOLLOW|unix.O_CLOEXEC, mode)
	if err != nil {
		return err
	}
	df := fdToFile(dstFd, dstName)
	defer df.Close()

	_, err = io.Copy(df, sf)
	return err
}

// copyDirAt recursively copies a directory using fd-based operations.
func copyDirAt(srcParentFd int, srcName string, dstParentFd int, dstName string) error {
	// Stat source directory for its mode.
	var srcStat unix.Stat_t
	if err := unix.Fstatat(srcParentFd, srcName, &srcStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}

	// Create destination directory.
	if err := unix.Mkdirat(dstParentFd, dstName, platformStatMode(&srcStat)&0o7777); err != nil && err != unix.EEXIST {
		return err
	}

	// Open both directories.
	srcDirFd, err := unix.Openat(srcParentFd, srcName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(srcDirFd)

	dstDirFd, err := unix.Openat(dstParentFd, dstName, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(dstDirFd)

	// Read source directory entries.
	dupFd, err := unix.Dup(srcDirFd)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(dupFd), srcName)
	entries, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		var stat unix.Stat_t
		if err := unix.Fstatat(srcDirFd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}

		if platformStatMode(&stat)&unix.S_IFMT == unix.S_IFDIR {
			if err := copyDirAt(srcDirFd, name, dstDirFd, name); err != nil {
				return err
			}
		} else {
			if err := copyFileAt(srcDirFd, name, dstDirFd, name, platformStatMode(&stat)&0o7777); err != nil {
				return err
			}
		}
	}

	return nil
}

// ── SetFileSize ──

func (sp *StorageProvider) opSetFileSize(corrID string, req *pb.SetFileSizeRequest) *pb.StorageOperationResponse {
	lock := sp.acquireFileLock(req.Path)
	if lock == nil {
		return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR, "file lock timeout")
	}
	defer sp.releaseFileLock(lock)

	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	fd, err := unix.Openat(dirFd, name, unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return osErrorResponse(corrID, err)
	}
	defer unix.Close(fd)

	if err := unix.Ftruncate(fd, int64(req.Size)); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

// ── SetPermissions ──

func (sp *StorageProvider) opSetPermissions(corrID string, req *pb.SetPermissionsRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	// Use Fchmodat with AT_SYMLINK_NOFOLLOW to prevent the leaf name from
	// being followed if it is a symlink (OpenParentDir already guards path
	// components). On Linux, fchmodat with AT_SYMLINK_NOFOLLOW returns ENOTSUP
	// for symlinks, which is correct — chmod on a symlink itself is meaningless.
	if err := unix.Fchmodat(dirFd, name, req.Mode, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

// ── SetTimes ──

func (sp *StorageProvider) opSetTimes(corrID string, req *pb.SetTimesRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	// Build utimensat timespec slice: [atime, mtime].
	// UTIME_OMIT means "don't change this time".
	ts := []unix.Timespec{
		{Sec: 0, Nsec: platformUtimeOmit()}, // atime
		{Sec: 0, Nsec: platformUtimeOmit()}, // mtime
	}

	if req.Atime != nil {
		t := req.Atime.AsTime()
		ts[0] = unix.NsecToTimespec(t.UnixNano())
	}
	if req.Mtime != nil {
		t := req.Mtime.AsTime()
		ts[1] = unix.NsecToTimespec(t.UnixNano())
	}

	if err := unix.UtimesNanoAt(dirFd, name, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

// ── Symlink ──

func (sp *StorageProvider) opSymlink(corrID string, req *pb.SymlinkRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.LinkPath)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	// target is passed through as-is (NFS allows arbitrary targets).
	if err := unix.Symlinkat(req.Target, dirFd, name); err != nil {
		return osErrorResponse(corrID, err)
	}

	return emptySuccessResponse(corrID)
}

// ── ReadLink ──

func (sp *StorageProvider) opReadLink(corrID string, req *pb.ReadLinkRequest) *pb.StorageOperationResponse {
	dirFd, name, err := sp.pathGuard.OpenParentDir(req.Path)
	if err != nil {
		return pathErrorResponse(corrID, err)
	}
	defer closeFdIfNotRoot(dirFd, sp.pathGuard.rootFd)

	buf := make([]byte, unix.PathMax)
	n, err := unix.Readlinkat(dirFd, name, buf)
	if err != nil {
		return osErrorResponse(corrID, err)
	}

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_ReadLink{
			ReadLink: &pb.ReadLinkData{Target: string(buf[:n])},
		},
	})
}

// ── StatFs ──

func (sp *StorageProvider) opStatFs(corrID string) *pb.StorageOperationResponse {
	var stat unix.Statfs_t
	if err := unix.Statfs(sp.pathGuard.rootPath, &stat); err != nil {
		return osErrorResponse(corrID, err)
	}

	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_StatFs{
			StatFs: &pb.StatFsData{
				Blocks:  stat.Blocks,
				Bfree:   stat.Bfree,
				Bavail:  stat.Bavail,
				Files:   stat.Files,
				Ffree:   stat.Ffree,
				Bsize:   uint32(stat.Bsize),
				Namelen: platformStatfsNamelen(&stat),
				Frsize:  platformStatfsFrsize(&stat),
			},
		},
	})
}

// ============================================================
// Response helpers
// ============================================================

func successResponse(corrID string, success *pb.StorageOperationSuccess) *pb.StorageOperationResponse {
	// Always mark non-paginated responses as the last (and only) page.
	// is_last defaults to false in protobuf, and the server uses is_last
	// to distinguish ListDir pagination pages from complete responses.
	success.IsLast = true
	return &pb.StorageOperationResponse{
		CorrelationId: corrID,
		Result: &pb.StorageOperationResponse_Success{
			Success: success,
		},
	}
}

func emptySuccessResponse(corrID string) *pb.StorageOperationResponse {
	return successResponse(corrID, &pb.StorageOperationSuccess{
		Data: &pb.StorageOperationSuccess_Empty{Empty: &pb.Empty{}},
	})
}

func errorResponse(corrID string, code pb.StorageErrorCode, msg string) *pb.StorageOperationResponse {
	return &pb.StorageOperationResponse{
		CorrelationId: corrID,
		Result: &pb.StorageOperationResponse_Error{
			Error: &pb.StorageOperationError{
				Code:    code,
				Message: msg,
			},
		},
	}
}

func pathErrorResponse(corrID string, err error) *pb.StorageOperationResponse {
	return errorResponse(corrID, pb.StorageErrorCode_STORAGE_ERROR_CODE_PATH_TRAVERSAL_DENIED, err.Error())
}

func osErrorResponse(corrID string, err error) *pb.StorageOperationResponse {
	code := pb.StorageErrorCode_STORAGE_ERROR_CODE_IO_ERROR
	// Check specific errno values before generic os.IsExist/os.IsNotExist,
	// because os.IsExist can match ENOTEMPTY on some platforms.
	switch {
	case err == unix.ENOTEMPTY:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_DIRECTORY_NOT_EMPTY
	case err == unix.EISDIR:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_IS_A_DIRECTORY
	case err == unix.ENOTDIR:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_NOT_A_DIRECTORY
	case err == unix.ELOOP:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_PATH_TRAVERSAL_DENIED
	case os.IsNotExist(err) || err == unix.ENOENT:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_NOT_FOUND
	case os.IsExist(err) || err == unix.EEXIST:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_ALREADY_EXISTS
	case os.IsPermission(err) || err == unix.EACCES:
		code = pb.StorageErrorCode_STORAGE_ERROR_CODE_PERMISSION_DENIED
	}
	return errorResponse(corrID, code, err.Error())
}

// ============================================================
// Stat conversion
// ============================================================

func statToProto(path, name string, stat *unix.Stat_t) *pb.FileStatData {
	var fileType uint32
	switch platformStatMode(stat) & unix.S_IFMT {
	case unix.S_IFDIR:
		fileType = 1 // Directory
	case unix.S_IFLNK:
		fileType = 2 // Symlink
	default:
		fileType = 0 // File
	}

	result := &pb.FileStatData{
		Name:     name,
		Path:     path,
		FileType: fileType,
		Size:     uint64(stat.Size),
		Mode:     platformStatMode(stat) & 0o7777, // permission bits only
		Uid:      stat.Uid,
		Gid:      stat.Gid,
	}

	// Modification time.
	if mtimSec, mtimNsec := platformStatMtime(stat); mtimSec > 0 {
		t := time.Unix(mtimSec, mtimNsec)
		result.ModifiedAt = timestamppb.New(t)
	}

	// Access time.
	if atimSec, atimNsec := platformStatAtime(stat); atimSec > 0 {
		t := time.Unix(atimSec, atimNsec)
		result.AccessedAt = timestamppb.New(t)
	}

	// Creation time (birth time) — Linux populates Ctim as ctime (inode change),
	// which is the best approximation.
	if ctimSec, ctimNsec := platformStatCtime(stat); ctimSec > 0 {
		t := time.Unix(ctimSec, ctimNsec)
		result.CreatedAt = timestamppb.New(t)
	}

	return result
}
