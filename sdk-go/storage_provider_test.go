package workspace

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/OpenElevo/ElevoWorkspace/sdk-go/proto/workspace/v1"
	"golang.org/x/sys/unix"
)

// ============================================================
// pathGuard tests
// ============================================================

func TestValidatePath_RejectsTraversal(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"double dot", ".."},
		{"leading double dot", "../etc/passwd"},
		{"mid double dot", "foo/../../etc/passwd"},
		{"absolute path", "/etc/passwd"},
	}

	dir := t.TempDir()
	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pg.ValidatePath(tt.path)
			if err == nil {
				t.Errorf("expected error for path %q, got nil", tt.path)
			}
		})
	}
}

func TestValidatePath_AllowsNormalPaths(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"simple file", "file.txt"},
		{"nested file", "src/main.rs"},
		{"deep path", "a/b/c/d/e.txt"},
		{"empty path (root)", ""},
		{"current dir", "."},
		{"dot in name", "foo.bar/baz.txt"},
	}

	dir := t.TempDir()
	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pg.ValidatePath(tt.path)
			if err != nil {
				t.Errorf("unexpected error for path %q: %v", tt.path, err)
			}
		})
	}
}

func TestOpenParentDir_SymlinkBlocked(t *testing.T) {
	dir := t.TempDir()

	// Create dir structure: dir/real/ and dir/link -> /tmp
	realDir := filepath.Join(dir, "real")
	if err := os.MkdirAll(realDir, 0755); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "link")
	if err := os.Symlink("/tmp", linkPath); err != nil {
		t.Fatal(err)
	}

	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	// Traversing through the symlink should fail.
	_, _, err = pg.OpenParentDir("link/somefile")
	if err == nil {
		t.Error("expected error when traversing symlink, got nil")
	}
}

func TestOpenParentDir_NormalAccess(t *testing.T) {
	dir := t.TempDir()

	// Create dir/sub/file.txt
	sub := filepath.Join(dir, "sub")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "file.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	// Open parent dir for "sub/file.txt"
	dirFd, name, err := pg.OpenParentDir("sub/file.txt")
	if err != nil {
		t.Fatalf("OpenParentDir failed: %v", err)
	}
	defer closeFdIfNotRoot(dirFd, pg.rootFd)

	if name != "file.txt" {
		t.Errorf("expected name 'file.txt', got %q", name)
	}

	// Verify we can read the file via the fd.
	fd, err := unix.Openat(dirFd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("openat failed: %v", err)
	}
	unix.Close(fd)
}

func TestOpenParentDir_RootFile(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "root.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()

	dirFd, name, err := pg.OpenParentDir("root.txt")
	if err != nil {
		t.Fatalf("OpenParentDir failed: %v", err)
	}

	if dirFd != pg.rootFd {
		t.Error("expected dirFd == rootFd for file in root dir")
		unix.Close(dirFd)
	}
	if name != "root.txt" {
		t.Errorf("expected name 'root.txt', got %q", name)
	}
}

// ============================================================
// chanMutex tests
// ============================================================

func TestChanMutex_BasicLockUnlock(t *testing.T) {
	mu := newChanMutex()

	// Acquire the lock.
	select {
	case <-mu.ch:
		// success
	default:
		t.Fatal("failed to acquire lock")
	}

	// Lock should now be unavailable.
	select {
	case <-mu.ch:
		t.Fatal("acquired lock while it should be held")
	default:
		// expected
	}

	// Release.
	mu.ch <- struct{}{}

	// Should be acquirable again.
	select {
	case <-mu.ch:
		// success
	default:
		t.Fatal("failed to acquire lock after release")
	}
}

func TestAcquireFileLock_ConcurrentWriteSerialization(t *testing.T) {
	sp := &StorageProvider{
		config: StorageProviderConfig{OperationTimeout: 5 * time.Second},
	}

	const numGoroutines = 10
	var counter int64
	var maxConcurrent int64
	var currentConcurrent int64

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock := sp.acquireFileLock("test-file.txt")
			if lock == nil {
				t.Error("failed to acquire lock")
				return
			}

			// Track concurrent access.
			cur := atomic.AddInt64(&currentConcurrent, 1)
			if cur > 1 {
				// Record if more than one goroutine is in the critical section.
				atomic.StoreInt64(&maxConcurrent, cur)
			}
			time.Sleep(time.Millisecond) // simulate work
			atomic.AddInt64(&currentConcurrent, -1)
			atomic.AddInt64(&counter, 1)

			sp.releaseFileLock(lock)
		}()
	}

	wg.Wait()

	if counter != numGoroutines {
		t.Errorf("expected counter=%d, got %d", numGoroutines, counter)
	}
	if maxConcurrent > 1 {
		t.Errorf("detected concurrent access: max_concurrent=%d", maxConcurrent)
	}
}

func TestAcquireFileLock_DifferentFilesIndependent(t *testing.T) {
	sp := &StorageProvider{
		config: StorageProviderConfig{OperationTimeout: 5 * time.Second},
	}

	// Acquire locks on two different files simultaneously.
	lock1 := sp.acquireFileLock("file-a.txt")
	if lock1 == nil {
		t.Fatal("failed to acquire lock for file-a")
	}

	lock2 := sp.acquireFileLock("file-b.txt")
	if lock2 == nil {
		t.Fatal("failed to acquire lock for file-b (should be independent)")
	}

	sp.releaseFileLock(lock1)
	sp.releaseFileLock(lock2)
}

func TestAcquireFileLock_Timeout(t *testing.T) {
	sp := &StorageProvider{
		config: StorageProviderConfig{OperationTimeout: 50 * time.Millisecond},
	}

	// Acquire the lock.
	lock := sp.acquireFileLock("busy-file.txt")
	if lock == nil {
		t.Fatal("first acquire failed")
	}

	// Second acquire should timeout.
	start := time.Now()
	lock2 := sp.acquireFileLock("busy-file.txt")
	elapsed := time.Since(start)

	if lock2 != nil {
		t.Error("second acquire should have timed out, but succeeded")
		sp.releaseFileLock(lock2)
	}

	if elapsed < 40*time.Millisecond {
		t.Errorf("timeout too fast: %v", elapsed)
	}

	sp.releaseFileLock(lock)
}

// ============================================================
// fileWatcher event coalescing tests
// ============================================================

func TestEventCoalescing_SamePathDedup(t *testing.T) {
	responseCh := make(chan *pb.ClientMessage, 100)

	dir := t.TempDir()
	fw := &fileWatcher{
		rootDir:       dir,
		responseCh:    responseCh,
		pendingEvents: make(map[string]*pb.FileChangeEvent),
		done:          make(chan struct{}),
	}

	// Simulate multiple events on the same path within 50ms.
	fw.pendingEvents["test.txt"] = &pb.FileChangeEvent{
		Path:      "test.txt",
		EventType: pb.FileChangeType_FILE_CHANGE_TYPE_CREATED,
	}
	fw.pendingEvents["test.txt"] = &pb.FileChangeEvent{
		Path:      "test.txt",
		EventType: pb.FileChangeType_FILE_CHANGE_TYPE_MODIFIED,
	}
	fw.flush()

	// Should receive exactly 1 event (last event wins).
	select {
	case msg := <-responseCh:
		fc := msg.GetFileChanged()
		if fc == nil {
			t.Fatal("expected FileChanged message")
		}
		if len(fc.Events) != 1 {
			t.Errorf("expected 1 event, got %d", len(fc.Events))
		}
		if fc.Events[0].EventType != pb.FileChangeType_FILE_CHANGE_TYPE_MODIFIED {
			t.Errorf("expected MODIFIED, got %v", fc.Events[0].EventType)
		}
	default:
		t.Fatal("expected message in channel")
	}
}

func TestEventCoalescing_MultiplePathsPreserved(t *testing.T) {
	responseCh := make(chan *pb.ClientMessage, 100)

	dir := t.TempDir()
	fw := &fileWatcher{
		rootDir:       dir,
		responseCh:    responseCh,
		pendingEvents: make(map[string]*pb.FileChangeEvent),
		done:          make(chan struct{}),
	}

	// Simulate events on different paths.
	fw.pendingEvents["a.txt"] = &pb.FileChangeEvent{
		Path:      "a.txt",
		EventType: pb.FileChangeType_FILE_CHANGE_TYPE_CREATED,
	}
	fw.pendingEvents["b.txt"] = &pb.FileChangeEvent{
		Path:      "b.txt",
		EventType: pb.FileChangeType_FILE_CHANGE_TYPE_DELETED,
	}
	fw.flush()

	select {
	case msg := <-responseCh:
		fc := msg.GetFileChanged()
		if fc == nil {
			t.Fatal("expected FileChanged message")
		}
		if len(fc.Events) != 2 {
			t.Errorf("expected 2 events, got %d", len(fc.Events))
		}
	default:
		t.Fatal("expected message in channel")
	}
}

func TestMapFsnotifyOp(t *testing.T) {
	tests := []struct {
		name     string
		op       fsnotify.Op
		expected pb.FileChangeType
	}{
		{"Create", fsnotify.Create, pb.FileChangeType_FILE_CHANGE_TYPE_CREATED},
		{"Write", fsnotify.Write, pb.FileChangeType_FILE_CHANGE_TYPE_MODIFIED},
		{"Remove", fsnotify.Remove, pb.FileChangeType_FILE_CHANGE_TYPE_DELETED},
		{"Rename", fsnotify.Rename, pb.FileChangeType_FILE_CHANGE_TYPE_RENAMED},
		{"Chmod", fsnotify.Chmod, pb.FileChangeType_FILE_CHANGE_TYPE_ATTR_CHANGED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapFsnotifyOp(tt.op)
			if got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

// ============================================================
// osErrorResponse mapping tests
// ============================================================

func TestOsErrorResponse_Mapping(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected pb.StorageErrorCode
	}{
		{"ENOENT", unix.ENOENT, pb.StorageErrorCode_STORAGE_ERROR_CODE_NOT_FOUND},
		{"EEXIST", unix.EEXIST, pb.StorageErrorCode_STORAGE_ERROR_CODE_ALREADY_EXISTS},
		{"EACCES", unix.EACCES, pb.StorageErrorCode_STORAGE_ERROR_CODE_PERMISSION_DENIED},
		{"EISDIR", unix.EISDIR, pb.StorageErrorCode_STORAGE_ERROR_CODE_IS_A_DIRECTORY},
		{"ENOTDIR", unix.ENOTDIR, pb.StorageErrorCode_STORAGE_ERROR_CODE_NOT_A_DIRECTORY},
		{"ENOTEMPTY", unix.ENOTEMPTY, pb.StorageErrorCode_STORAGE_ERROR_CODE_DIRECTORY_NOT_EMPTY},
		{"ELOOP", unix.ELOOP, pb.StorageErrorCode_STORAGE_ERROR_CODE_PATH_TRAVERSAL_DENIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := osErrorResponse("test-corr", tt.err)
			errResult := resp.GetError()
			if errResult == nil {
				t.Fatal("expected error response")
			}
			if errResult.Code != tt.expected {
				t.Errorf("expected code %v, got %v", tt.expected, errResult.Code)
			}
		})
	}
}

// ============================================================
// Ignore rules tests
// ============================================================

func TestIsIgnored_DefaultDirs(t *testing.T) {
	dir := t.TempDir()
	fw := &fileWatcher{
		rootDir: dir,
		done:    make(chan struct{}),
	}

	tests := []struct {
		path    string
		ignored bool
	}{
		{filepath.Join(dir, ".git", "objects", "pack"), true},
		{filepath.Join(dir, "node_modules", "express"), true},
		{filepath.Join(dir, "__pycache__", "module.pyc"), true},
		{filepath.Join(dir, "src", "main.rs"), false},
		{filepath.Join(dir, "target", "debug"), true},
	}

	for _, tt := range tests {
		name, _ := filepath.Rel(dir, tt.path)
		t.Run(name, func(t *testing.T) {
			if got := fw.isIgnored(tt.path); got != tt.ignored {
				t.Errorf("isIgnored(%q) = %v, want %v", tt.path, got, tt.ignored)
			}
		})
	}
}

// ============================================================
// Storage operation tests (integration with real filesystem)
// ============================================================

func TestOpStat(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opStat("corr-1", &pb.StatRequest{Path: "test.txt"})
	if resp.GetError() != nil {
		t.Fatalf("unexpected error: %v", resp.GetError())
	}

	stat := resp.GetSuccess().GetStat()
	if stat == nil {
		t.Fatal("expected stat result")
	}
	if stat.Name != "test.txt" {
		t.Errorf("expected name 'test.txt', got %q", stat.Name)
	}
	if stat.Size != 5 {
		t.Errorf("expected size 5, got %d", stat.Size)
	}
	if stat.FileType != 0 { // File
		t.Errorf("expected file_type 0 (File), got %d", stat.FileType)
	}
}

func TestOpExists(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "exists.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// File that exists.
	resp := sp.opExists("corr-1", &pb.ExistsRequest{Path: "exists.txt"})
	if !resp.GetSuccess().GetExists().GetExists() {
		t.Error("expected file to exist")
	}

	// File that doesn't exist.
	resp = sp.opExists("corr-2", &pb.ExistsRequest{Path: "nope.txt"})
	if resp.GetSuccess().GetExists().GetExists() {
		t.Error("expected file to not exist")
	}
}

func TestOpCreateFile(t *testing.T) {
	dir := t.TempDir()

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Create a new file.
	resp := sp.opCreateFile("corr-1", &pb.CreateFileRequest{Path: "new.txt", Exclusive: true})
	if resp.GetError() != nil {
		t.Fatalf("create failed: %v", resp.GetError())
	}

	// Verify it exists.
	if _, err := os.Stat(filepath.Join(dir, "new.txt")); err != nil {
		t.Errorf("file should exist: %v", err)
	}

	// Exclusive create should fail.
	resp = sp.opCreateFile("corr-2", &pb.CreateFileRequest{Path: "new.txt", Exclusive: true})
	if resp.GetError() == nil {
		t.Error("expected error for exclusive create on existing file")
	}
	if resp.GetError().Code != pb.StorageErrorCode_STORAGE_ERROR_CODE_ALREADY_EXISTS {
		t.Errorf("expected ALREADY_EXISTS, got %v", resp.GetError().Code)
	}
}

func TestOpMkdir(t *testing.T) {
	dir := t.TempDir()

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Non-recursive mkdir.
	resp := sp.opMkdir("corr-1", &pb.StorageMkdirRequest{Path: "subdir", Recursive: false})
	if resp.GetError() != nil {
		t.Fatalf("mkdir failed: %v", resp.GetError())
	}

	// Recursive mkdir.
	resp = sp.opMkdir("corr-2", &pb.StorageMkdirRequest{Path: "a/b/c", Recursive: true})
	if resp.GetError() != nil {
		t.Fatalf("recursive mkdir failed: %v", resp.GetError())
	}

	if _, err := os.Stat(filepath.Join(dir, "a", "b", "c")); err != nil {
		t.Errorf("directory should exist: %v", err)
	}
}

func TestOpReadFileRange(t *testing.T) {
	dir := t.TempDir()
	data := []byte("Hello, World!")
	if err := os.WriteFile(filepath.Join(dir, "data.txt"), data, 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Read bytes 7-12 ("World").
	resp := sp.opReadFileRange("corr-1", &pb.ReadFileRangeRequest{
		Path:   "data.txt",
		Offset: 7,
		Length: 5,
	})
	if resp.GetError() != nil {
		t.Fatalf("read range failed: %v", resp.GetError())
	}

	got := string(resp.GetSuccess().GetReadData().GetData())
	if got != "World" {
		t.Errorf("expected 'World', got %q", got)
	}
}

func TestOpReadFileRange_EntireFile(t *testing.T) {
	dir := t.TempDir()
	data := []byte("Read the entire file content here.")
	if err := os.WriteFile(filepath.Join(dir, "full.txt"), data, 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// length=0 means "read entire file".
	resp := sp.opReadFileRange("corr-full", &pb.ReadFileRangeRequest{
		Path:   "full.txt",
		Offset: 0,
		Length: 0,
	})
	if resp.GetError() != nil {
		t.Fatalf("read entire file failed: %v", resp.GetError())
	}

	got := resp.GetSuccess().GetReadData().GetData()
	if string(got) != string(data) {
		t.Errorf("expected %q, got %q", string(data), string(got))
	}
}

func TestOpReadFileRange_EntireFileWithOffset(t *testing.T) {
	dir := t.TempDir()
	data := []byte("0123456789ABCDEF")
	if err := os.WriteFile(filepath.Join(dir, "offset.txt"), data, 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// length=0 with offset=10 should read from offset to end.
	resp := sp.opReadFileRange("corr-offset", &pb.ReadFileRangeRequest{
		Path:   "offset.txt",
		Offset: 10,
		Length: 0,
	})
	if resp.GetError() != nil {
		t.Fatalf("read entire file with offset failed: %v", resp.GetError())
	}

	got := resp.GetSuccess().GetReadData().GetData()
	if string(got) != "ABCDEF" {
		t.Errorf("expected %q, got %q", "ABCDEF", string(got))
	}
}

func TestOpWriteFileAt(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "out.txt"), []byte("AAAAAAAAAA"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Overwrite bytes at offset 5.
	resp := sp.opWriteFileAt("corr-1", &pb.WriteFileAtRequest{
		Path:   "out.txt",
		Offset: 5,
		Data:   []byte("BBBBB"),
	})
	if resp.GetError() != nil {
		t.Fatalf("write at failed: %v", resp.GetError())
	}

	content, err := os.ReadFile(filepath.Join(dir, "out.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "AAAAABBBBB" {
		t.Errorf("expected 'AAAAABBBBB', got %q", string(content))
	}
}

func TestOpRemoveFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "to-delete.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opRemoveFile("corr-1", &pb.RemoveFileRequest{Path: "to-delete.txt"})
	if resp.GetError() != nil {
		t.Fatalf("remove file failed: %v", resp.GetError())
	}

	if _, err := os.Stat(filepath.Join(dir, "to-delete.txt")); !os.IsNotExist(err) {
		t.Error("file should have been deleted")
	}
}

func TestOpRemoveDir_Recursive(t *testing.T) {
	dir := t.TempDir()

	// Create dir/sub/nested/file.txt
	nested := filepath.Join(dir, "sub", "nested")
	if err := os.MkdirAll(nested, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "file.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opRemoveDir("corr-1", &pb.RemoveDirRequest{Path: "sub", Recursive: true})
	if resp.GetError() != nil {
		t.Fatalf("remove dir recursive failed: %v", resp.GetError())
	}

	if _, err := os.Stat(filepath.Join(dir, "sub")); !os.IsNotExist(err) {
		t.Error("directory should have been deleted")
	}
}

func TestOpRename(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "old.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opRename("corr-1", &pb.RenameRequest{Src: "old.txt", Dst: "new.txt"})
	if resp.GetError() != nil {
		t.Fatalf("rename failed: %v", resp.GetError())
	}

	if _, err := os.Stat(filepath.Join(dir, "old.txt")); !os.IsNotExist(err) {
		t.Error("old file should not exist")
	}

	content, err := os.ReadFile(filepath.Join(dir, "new.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "data" {
		t.Errorf("expected 'data', got %q", string(content))
	}
}

func TestOpRename_Noreplace(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "src.txt"), []byte("src"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dst.txt"), []byte("dst"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opRename("corr-1", &pb.RenameRequest{Src: "src.txt", Dst: "dst.txt", Flags: 1}) // NOREPLACE
	if resp.GetError() == nil {
		t.Error("expected error for NOREPLACE rename with existing destination")
	}
	if resp.GetError().Code != pb.StorageErrorCode_STORAGE_ERROR_CODE_ALREADY_EXISTS {
		t.Errorf("expected ALREADY_EXISTS, got %v", resp.GetError().Code)
	}
}

func TestOpCopy(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "original.txt"), []byte("copy me"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opCopy("corr-1", &pb.CopyRequest{Src: "original.txt", Dst: "copied.txt"})
	if resp.GetError() != nil {
		t.Fatalf("copy failed: %v", resp.GetError())
	}

	content, err := os.ReadFile(filepath.Join(dir, "copied.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "copy me" {
		t.Errorf("expected 'copy me', got %q", string(content))
	}

	// Original should still exist.
	if _, err := os.Stat(filepath.Join(dir, "original.txt")); err != nil {
		t.Error("original file should still exist")
	}
}

func TestOpSetFileSize(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "trunc.txt"), []byte("hello world"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opSetFileSize("corr-1", &pb.SetFileSizeRequest{Path: "trunc.txt", Size: 5})
	if resp.GetError() != nil {
		t.Fatalf("truncate failed: %v", resp.GetError())
	}

	content, err := os.ReadFile(filepath.Join(dir, "trunc.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "hello" {
		t.Errorf("expected 'hello', got %q", string(content))
	}
}

func TestOpSymlinkAndReadLink(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "target.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opSymlink("corr-1", &pb.SymlinkRequest{LinkPath: "link.txt", Target: "target.txt"})
	if resp.GetError() != nil {
		t.Fatalf("symlink failed: %v", resp.GetError())
	}

	resp = sp.opReadLink("corr-2", &pb.ReadLinkRequest{Path: "link.txt"})
	if resp.GetError() != nil {
		t.Fatalf("readlink failed: %v", resp.GetError())
	}

	target := resp.GetSuccess().GetReadLink().GetTarget()
	if target != "target.txt" {
		t.Errorf("expected target 'target.txt', got %q", target)
	}
}

func TestOpStatFs(t *testing.T) {
	dir := t.TempDir()

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opStatFs("corr-1")
	if resp.GetError() != nil {
		t.Fatalf("statfs failed: %v", resp.GetError())
	}

	statFs := resp.GetSuccess().GetStatFs()
	if statFs == nil {
		t.Fatal("expected statfs data")
	}
	if statFs.Bsize == 0 {
		t.Error("expected non-zero bsize")
	}
	if statFs.Blocks == 0 {
		t.Error("expected non-zero blocks")
	}
}

func TestOpListDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("b"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opListDir("corr-1", &pb.ListDirRequest{Path: ""})
	if resp.GetError() != nil {
		t.Fatalf("list dir failed: %v", resp.GetError())
	}

	entries := resp.GetSuccess().GetListDir().GetEntries()
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, expected := range []string{"a.txt", "b.txt", "sub"} {
		if !names[expected] {
			t.Errorf("missing entry: %s", expected)
		}
	}
}

func TestOpSetPermissions(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "perm.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opSetPermissions("corr-1", &pb.SetPermissionsRequest{Path: "perm.txt", Mode: 0755})
	if resp.GetError() != nil {
		t.Fatalf("set permissions failed: %v", resp.GetError())
	}

	info, err := os.Stat(filepath.Join(dir, "perm.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0755 {
		t.Errorf("expected mode 0755, got %o", info.Mode().Perm())
	}
}

// ============================================================
// Missing tests identified by audit
// ============================================================

func TestOpSetTimes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "times.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Set both atime and mtime to specific values.
	atime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	mtime := time.Date(2025, 7, 20, 8, 30, 0, 0, time.UTC)

	resp := sp.opSetTimes("corr-1", &pb.SetTimesRequest{
		Path:  "times.txt",
		Atime: timestamppb.New(atime),
		Mtime: timestamppb.New(mtime),
	})
	if resp.GetError() != nil {
		t.Fatalf("set times failed: %v", resp.GetError())
	}

	// Verify via stat.
	var stat unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, "times.txt"), &stat); err != nil {
		t.Fatal(err)
	}

	mtimSec, mtimNsec := platformStatMtime(&stat)
	gotMtime := time.Unix(mtimSec, mtimNsec)
	if !gotMtime.Equal(mtime) {
		t.Errorf("mtime mismatch: got %v, want %v", gotMtime, mtime)
	}

	atimSec, atimNsec := platformStatAtime(&stat)
	gotAtime := time.Unix(atimSec, atimNsec)
	if !gotAtime.Equal(atime) {
		t.Errorf("atime mismatch: got %v, want %v", gotAtime, atime)
	}
}

func TestOpSetTimes_OmitOne(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "times2.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// Get original times.
	var origStat unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, "times2.txt"), &origStat); err != nil {
		t.Fatal(err)
	}

	// Set only mtime, leave atime unchanged (nil).
	mtime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	resp := sp.opSetTimes("corr-1", &pb.SetTimesRequest{
		Path:  "times2.txt",
		Mtime: timestamppb.New(mtime),
		// Atime is nil → UTIME_OMIT
	})
	if resp.GetError() != nil {
		t.Fatalf("set times failed: %v", resp.GetError())
	}

	var newStat unix.Stat_t
	if err := unix.Stat(filepath.Join(dir, "times2.txt"), &newStat); err != nil {
		t.Fatal(err)
	}

	newMtimSec, newMtimNsec := platformStatMtime(&newStat)
	gotMtime := time.Unix(newMtimSec, newMtimNsec)
	if !gotMtime.Equal(mtime) {
		t.Errorf("mtime mismatch: got %v, want %v", gotMtime, mtime)
	}
}

func TestOpRename_Exchange(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "alpha.txt"), []byte("AAA"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "beta.txt"), []byte("BBB"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	// EXCHANGE flag = 2 (atomically swap two files).
	resp := sp.opRename("corr-1", &pb.RenameRequest{Src: "alpha.txt", Dst: "beta.txt", Flags: 2})
	if resp.GetError() != nil {
		t.Fatalf("rename exchange failed: %v", resp.GetError())
	}

	// After exchange: alpha.txt should contain "BBB", beta.txt should contain "AAA".
	alphaContent, err := os.ReadFile(filepath.Join(dir, "alpha.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(alphaContent) != "BBB" {
		t.Errorf("expected alpha.txt to contain 'BBB', got %q", string(alphaContent))
	}

	betaContent, err := os.ReadFile(filepath.Join(dir, "beta.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(betaContent) != "AAA" {
		t.Errorf("expected beta.txt to contain 'AAA', got %q", string(betaContent))
	}
}

func TestOpCopy_Directory(t *testing.T) {
	dir := t.TempDir()

	// Create source directory structure: dir/srcdir/a.txt, dir/srcdir/sub/b.txt
	srcDir := filepath.Join(dir, "srcdir")
	if err := os.MkdirAll(filepath.Join(srcDir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("aaa"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "sub", "b.txt"), []byte("bbb"), 0644); err != nil {
		t.Fatal(err)
	}

	sp := newTestStorageProvider(t, dir)
	defer sp.pathGuard.Close()

	resp := sp.opCopy("corr-1", &pb.CopyRequest{Src: "srcdir", Dst: "dstdir"})
	if resp.GetError() != nil {
		t.Fatalf("directory copy failed: %v", resp.GetError())
	}

	// Verify the copied directory structure.
	dstDir := filepath.Join(dir, "dstdir")

	content, err := os.ReadFile(filepath.Join(dstDir, "a.txt"))
	if err != nil {
		t.Fatalf("copied a.txt not found: %v", err)
	}
	if string(content) != "aaa" {
		t.Errorf("expected 'aaa', got %q", string(content))
	}

	content, err = os.ReadFile(filepath.Join(dstDir, "sub", "b.txt"))
	if err != nil {
		t.Fatalf("copied sub/b.txt not found: %v", err)
	}
	if string(content) != "bbb" {
		t.Errorf("expected 'bbb', got %q", string(content))
	}

	// Original should still exist.
	if _, err := os.Stat(filepath.Join(srcDir, "a.txt")); err != nil {
		t.Error("original should still exist")
	}
}

func TestIsIgnored_AllDefaultDirs(t *testing.T) {
	dir := t.TempDir()
	fw := &fileWatcher{
		rootDir: dir,
		done:    make(chan struct{}),
	}

	// Test ALL default ignore directories.
	for ignoredDir := range defaultIgnoreDirs {
		path := filepath.Join(dir, ignoredDir, "subpath")
		t.Run(ignoredDir, func(t *testing.T) {
			if !fw.isIgnored(path) {
				t.Errorf("expected %q to be ignored", ignoredDir)
			}
		})
	}

	// Verify non-ignored paths.
	nonIgnored := []string{"src", "lib", "pkg", "docs", "README.md"}
	for _, name := range nonIgnored {
		path := filepath.Join(dir, name)
		t.Run("not_"+name, func(t *testing.T) {
			if fw.isIgnored(path) {
				t.Errorf("expected %q to NOT be ignored", name)
			}
		})
	}
}

func TestIsIgnored_ElevoignoreRules(t *testing.T) {
	dir := t.TempDir()

	// Create .elevoignore file with rules.
	ignoreContent := "*.log\ntmp\n# comment line\n\n*.bak\n"
	if err := os.WriteFile(filepath.Join(dir, ".elevoignore"), []byte(ignoreContent), 0644); err != nil {
		t.Fatal(err)
	}

	rules := loadElevoIgnore(dir)
	fw := &fileWatcher{
		rootDir:     dir,
		ignoreRules: rules,
		done:        make(chan struct{}),
	}

	tests := []struct {
		path    string
		ignored bool
	}{
		{filepath.Join(dir, "app.log"), true},
		{filepath.Join(dir, "debug.log"), true},
		{filepath.Join(dir, "main.go"), false},
		{filepath.Join(dir, "backup.bak"), true},
		{filepath.Join(dir, "src", "main.go"), false},
	}

	for _, tt := range tests {
		name := filepath.Base(tt.path)
		t.Run(name, func(t *testing.T) {
			if got := fw.isIgnored(tt.path); got != tt.ignored {
				t.Errorf("isIgnored(%q) = %v, want %v", tt.path, got, tt.ignored)
			}
		})
	}
}

func TestStatToProto_FileTypes(t *testing.T) {
	dir := t.TempDir()

	// Create regular file.
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	// Create directory.
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	// Create symlink.
	if err := os.Symlink("file.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		path         string
		expectedType uint32
	}{
		{"regular file", "file.txt", 0},
		{"directory", "sub", 1},
		{"symlink", "link", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stat unix.Stat_t
			if err := unix.Lstat(filepath.Join(dir, tt.path), &stat); err != nil {
				t.Fatal(err)
			}
			proto := statToProto(tt.path, tt.path, &stat)
			if proto.FileType != tt.expectedType {
				t.Errorf("expected file_type %d, got %d", tt.expectedType, proto.FileType)
			}
		})
	}
}

// ============================================================
// Test helpers
// ============================================================

func newTestStorageProvider(t *testing.T, dir string) *StorageProvider {
	t.Helper()
	pg, err := newPathGuard(dir)
	if err != nil {
		t.Fatal(err)
	}

	return &StorageProvider{
		config: StorageProviderConfig{
			LocalDir:         dir,
			OperationTimeout: 5 * time.Second,
		},
		pathGuard:  pg,
		responseCh: make(chan *pb.ClientMessage, 256),
	}
}
