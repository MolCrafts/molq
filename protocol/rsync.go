package protocol

import "strings"

// rsyncValueLong names the long rsync options that take a value. A value is
// carried inside the option word ("--exclude=build/") on the wire, so a
// detached value can never consume the operand separator.
var rsyncValueLong = map[string]bool{
	"--address": true, "--backup-dir": true, "--block-size": true, "--bwlimit": true,
	"--cc": true, "--checksum-choice": true, "--checksum-seed": true, "--chmod": true,
	"--chown": true, "--compare-dest": true, "--compress-choice": true,
	"--compress-level": true, "--config": true, "--contimeout": true, "--copy-as": true,
	"--copy-dest": true, "--debug": true, "--dparam": true, "--early-input": true,
	"--exclude": true, "--exclude-from": true, "--files-from": true, "--filter": true,
	"--groupmap": true, "--iconv": true, "--include": true, "--include-from": true,
	"--info": true, "--link-dest": true, "--log-file": true, "--log-file-format": true,
	"--max-alloc": true, "--max-delete": true, "--max-size": true, "--min-size": true,
	"--modify-window": true, "--only-write-batch": true, "--out-format": true,
	"--outbuf": true, "--partial-dir": true, "--password-file": true, "--port": true,
	"--protocol": true, "--read-batch": true, "--remote-option": true, "--rsh": true,
	"--rsync-path": true, "--skip-compress": true, "--sockopts": true, "--stderr": true,
	"--stop-after": true, "--stop-at": true, "--suffix": true, "--temp-dir": true,
	"--timeout": true, "--usermap": true, "--write-batch": true, "--zc": true, "--zl": true,
}

// rsyncValueShort maps short options that take a value to their long names.
var rsyncValueShort = map[byte]string{
	'B': "--block-size", 'e': "--rsh", 'f': "--filter", 'M': "--remote-option",
	'T': "--temp-dir", '@': "--modify-window",
}

// rsyncOwned options decide how rsync reaches a host or which program runs
// there. Transport owns that, so callers cannot replace it.
var rsyncOwned = map[string]bool{
	"--rsh": true, "--rsync-path": true, "--remote-option": true, "--daemon": true,
	"--server": true, "--sender": true, "--config": true, "--dparam": true,
	"--password-file": true, "--early-input": true,
}

// SplitRsyncArgs splits an rsync command line into option words and operands.
// Detached values are attached, and short value options become their long
// form, so every returned option passes CheckRsyncOption on its own.
func SplitRsyncArgs(args []string) ([]string, []string, error) {
	var options, operands []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--":
			return options, append(operands, args[i+1:]...), nil
		case strings.HasPrefix(arg, "--"):
			if rsyncValueLong[arg] {
				if i+1 >= len(args) {
					return nil, nil, Fail("INVALID_INPUT", "rsync option requires a value", map[string]any{"option": arg})
				}
				i++
				arg += "=" + args[i]
			}
			options = append(options, arg)
		case strings.HasPrefix(arg, "-") && len(arg) > 1:
			for j := 1; j < len(arg); j++ {
				long, ok := rsyncValueShort[arg[j]]
				if !ok {
					continue
				}
				value := arg[j+1:]
				if value == "" {
					if i+1 >= len(args) {
						return nil, nil, Fail("INVALID_INPUT", "rsync option requires a value", map[string]any{"option": "-" + string(arg[j])})
					}
					i++
					value = args[i]
				}
				if j > 1 {
					options = append(options, arg[:j])
				}
				arg = long + "=" + value
				break
			}
			options = append(options, arg)
		default:
			operands = append(operands, arg)
		}
	}
	return options, operands, nil
}

// CheckRsyncOption accepts one self-contained rsync option word. It rejects
// only what Transport owns or what could swallow an operand; rsync itself
// judges every other option.
func CheckRsyncOption(option string) error {
	reject := func(message string) error {
		return Fail("INVALID_INPUT", message, map[string]any{"option": option})
	}
	if option == "--" || !strings.HasPrefix(option, "-") || len(option) < 2 || strings.ContainsAny(option, "\n\r") {
		return reject("rsync options are single-line words starting with '-'")
	}
	if strings.HasPrefix(option, "--") {
		name, _, attached := strings.Cut(option, "=")
		if rsyncOwned[name] {
			return reject("Transport owns how rsync reaches the target")
		}
		if rsyncValueLong[name] && !attached {
			return reject("rsync option value must be attached as --name=value")
		}
		return nil
	}
	for j := 1; j < len(option); j++ {
		long, ok := rsyncValueShort[option[j]]
		if !ok {
			continue
		}
		if rsyncOwned[long] {
			return reject("Transport owns how rsync reaches the target")
		}
		if j+1 == len(option) {
			return reject("rsync option value must be attached")
		}
		return nil
	}
	return nil
}
