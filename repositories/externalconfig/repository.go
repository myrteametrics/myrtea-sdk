package externalconfig

import (
	"sync"
)

// Repository is a storage interface which can be implemented by multiple backend
// (in-memory map, sql database, in-memory cache, file system, ...)
// It allows standard CRUD operation on ExternalConfigs and Folders
type Repository interface {
	Get(id int64) (ExternalConfig, bool, error)
	GetByName(name string) (ExternalConfig, bool, error)
	Create(config ExternalConfig) (int64, error)
	Update(id int64, config ExternalConfig) error
	Delete(id int64) error
	GetAll() (map[int64]ExternalConfig, error)
	GetAllOldVersions(id int64) ([]ExternalConfig, error)
	// MoveConfig moves an ExternalConfig to a different folder (nil = root).
	MoveConfig(id int64, newFolderId *int64) error

	// Folder operations
	CreateFolder(folder Folder) (int64, error)
	GetFolder(id int64) (Folder, bool, error)
	GetFolderByName(name string, parentId *int64) (Folder, bool, error)
	UpdateFolder(id int64, folder Folder) error
	DeleteFolder(id int64) error
	GetAllFolders() (map[int64]Folder, error)
	// GetFolderHierarchy returns the direct child folders and the configs that live
	// directly at the requested level (parentId == nil → root).
	GetFolderHierarchy(parentId *int64) (HierarchyResult, error)
	// MoveFolder moves a folder to a new parent (nil = root).
	// Returns an error if the move would create a circular reference.
	MoveFolder(id int64, newParentId *int64) error
	// GetFolderParentCount returns the depth of a folder in the hierarchy (0 = root).
	GetFolderParentCount(folderId int64) (int, error)
}

var (
	_globalRepositoryMu sync.RWMutex
	_globalRepository   Repository
)

// R is used to access the global repository singleton
func R() Repository {
	_globalRepositoryMu.RLock()
	defer _globalRepositoryMu.RUnlock()

	repository := _globalRepository
	return repository
}

// ReplaceGlobals affect a new repository to the global repository singleton
func ReplaceGlobals(repository Repository) func() {
	_globalRepositoryMu.Lock()
	defer _globalRepositoryMu.Unlock()

	prev := _globalRepository
	_globalRepository = repository
	return func() { ReplaceGlobals(prev) }
}
