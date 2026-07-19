//go:build darwin && !ios && !server

package application

/*
#include <stdint.h>
*/
import "C"

//export restoreWindowSizeConstraints
func restoreWindowSizeConstraints(windowId C.uint) {
	window, _ := globalApplication.Window.GetByID(uint(windowId))
	webviewWindow, ok := window.(*WebviewWindow)
	if !ok {
		return
	}
	webviewWindow.restoreNativeSizeConstraints()
}
