//go:build windows

package transport

import "os/exec"

func configureCommand(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Kill()
	}
}

func nonBlockingFileFlag() int { return 0 }
