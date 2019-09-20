package cmd

import (
	"testing"
	"time"
)

func TestDiskMaintenanceResultsDisplay(t *testing.T) {
	results := diskMaintenanceResults{
		diskMaintenanceResult{
			Endpoint:      "192.168.4.7",
			Status:        MaintenanceStatusIdle,
			CurrentJob:    "/data1/foo",
			CurrentStatus: "dumping",
			Completed:     1,
			Total:         10,
			Message:       "",
			LastUpdated:   time.Now(),
		},
		diskMaintenanceResult{
			Endpoint:      "192.168.4.8",
			Status:        MaintenanceStatusRunning,
			CurrentJob:    "/data1/foo",
			CurrentStatus: "dumping",
			Completed:     2,
			Total:         10,
			Message:       "",
			LastUpdated:   time.Now(),
		},
		diskMaintenanceResult{
			Endpoint:      "192.168.4.9",
			Status:        MaintenanceStatusSucceeded,
			CurrentJob:    "/data1/foo",
			CurrentStatus: "dumping",
			Completed:     10,
			Total:         10,
			Message:       "",
			LastUpdated:   time.Now(),
		},
		diskMaintenanceResult{
			Endpoint:      "192.168.4.6",
			Status:        MaintenanceStatusFailed,
			CurrentJob:    "/data1/foo",
			CurrentStatus: "dumping",
			Completed:     1,
			Total:         10,
			Message:       "fucked",
			LastUpdated:   time.Now(),
		},
	}
	results.display()
}
