//go:build darwin

package workspace

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// platformRenameat2 falls back to unix.Renameat on macOS.
// macOS does not support RENAME_NOREPLACE or RENAME_EXCHANGE natively via renameat2.
// If flags are non-zero, we return an error indicating the operation is unsupported.
func platformRenameat2(oldDirFd int, oldName string, newDirFd int, newName string, flags uint) error {
	if flags != 0 {
		return fmt.Errorf("renameat2 flags (0x%x) not supported on darwin", flags)
	}
	return unix.Renameat(oldDirFd, oldName, newDirFd, newName)
}

// platformRenameNoreplace returns a placeholder flag value on macOS.
// The actual renameat2 call will return an error if this flag is used.
func platformRenameNoreplace() uint {
	return 1 // matches Linux RENAME_NOREPLACE value
}

// platformRenameExchange returns a placeholder flag value on macOS.
// The actual renameat2 call will return an error if this flag is used.
func platformRenameExchange() uint {
	return 2 // matches Linux RENAME_EXCHANGE value
}

// platformUtimeOmit returns a value that signals "do not change this time" on macOS.
// On Linux this is the kernel's UTIME_OMIT constant ((1 << 30) - 2).
// We define it manually here for compilation compatibility.
func platformUtimeOmit() int64 {
	return (1 << 30) - 2 // 0x3FFFFFFE, same value as Linux UTIME_OMIT
}

// platformStatMode returns stat.Mode as uint32.
// On darwin, Stat_t.Mode is uint16, so we cast it.
func platformStatMode(stat *unix.Stat_t) uint32 {
	return uint32(stat.Mode)
}

// platformStatMtime returns the modification time fields from Stat_t.
// In golang.org/x/sys v0.38+, darwin Stat_t uses Mtim (same as Linux).
func platformStatMtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Mtim.Sec, stat.Mtim.Nsec
}

// platformStatAtime returns the access time fields from Stat_t.
// In golang.org/x/sys v0.38+, darwin Stat_t uses Atim (same as Linux).
func platformStatAtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Atim.Sec, stat.Atim.Nsec
}

// platformStatCtime returns the change time fields from Stat_t.
// In golang.org/x/sys v0.38+, darwin Stat_t uses Ctim (same as Linux).
func platformStatCtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Ctim.Sec, stat.Ctim.Nsec
}

// platformStatfsNamelen returns a default max filename length on macOS.
// darwin's Statfs_t does not have a Namelen field; NAME_MAX is typically 255.
func platformStatfsNamelen(stat *unix.Statfs_t) uint32 {
	return 255
}

// platformStatfsFrsize returns the fragment size on macOS.
// darwin's Statfs_t does not have a Frsize field; Bsize is the best approximation.
func platformStatfsFrsize(stat *unix.Statfs_t) uint32 {
	return uint32(stat.Bsize)
}
