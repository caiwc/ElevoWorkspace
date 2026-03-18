//go:build linux

package workspace

import "golang.org/x/sys/unix"

// platformRenameat2 wraps unix.Renameat2 which supports RENAME_NOREPLACE and RENAME_EXCHANGE on Linux.
func platformRenameat2(oldDirFd int, oldName string, newDirFd int, newName string, flags uint) error {
	return unix.Renameat2(oldDirFd, oldName, newDirFd, newName, flags)
}

// platformRenameNoreplace returns the Linux RENAME_NOREPLACE flag.
func platformRenameNoreplace() uint {
	return unix.RENAME_NOREPLACE
}

// platformRenameExchange returns the Linux RENAME_EXCHANGE flag.
func platformRenameExchange() uint {
	return unix.RENAME_EXCHANGE
}

// platformUtimeOmit returns unix.UTIME_OMIT for Linux.
func platformUtimeOmit() int64 {
	return unix.UTIME_OMIT
}

// platformStatMode returns stat.Mode as uint32 (already uint32 on Linux).
func platformStatMode(stat *unix.Stat_t) uint32 {
	return stat.Mode
}

// platformStatMtime returns the modification time fields from Stat_t.
func platformStatMtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Mtim.Sec, stat.Mtim.Nsec
}

// platformStatAtime returns the access time fields from Stat_t.
func platformStatAtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Atim.Sec, stat.Atim.Nsec
}

// platformStatCtime returns the change time fields from Stat_t.
func platformStatCtime(stat *unix.Stat_t) (sec, nsec int64) {
	return stat.Ctim.Sec, stat.Ctim.Nsec
}

// platformStatfsNamelen returns the maximum filename length from Statfs_t.
func platformStatfsNamelen(stat *unix.Statfs_t) uint32 {
	return uint32(stat.Namelen)
}

// platformStatfsFrsize returns the fragment size from Statfs_t.
func platformStatfsFrsize(stat *unix.Statfs_t) uint32 {
	return uint32(stat.Frsize)
}
