package externalconfig

import "time"

// Folder represents a folder in the external config hierarchy
type Folder struct {
	Id        int64     `json:"id"`
	Name      string    `json:"name"`
	ParentId  *int64    `json:"parentId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// FolderNode represents a folder with its children for tree structure
type FolderNode struct {
	Folder
	SubFolders []FolderNode     `json:"subFolders"`
	Configs    []ExternalConfig `json:"configs"`
}

// HierarchyResult holds one level of the folder hierarchy.
// Folders contains the direct child folders; Configs contains the configs
// that live directly at this level (folder_id = parentId, or NULL for root).
type HierarchyResult struct {
	Folders []FolderNode     `json:"folders"`
	Configs []ExternalConfig `json:"configs"`
}
