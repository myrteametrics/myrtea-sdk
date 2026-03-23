package externalconfig

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/repositories/utils"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

const (
	table         = "external_generic_config_v1"
	versionsTable = "external_generic_config_versions_v1"
	foldersTable  = "external_config_folders_v1"
)

// PostgresRepository is a repository containing the ExternalConfig definition based on a PSQL database and
// implementing the repository interface
type PostgresRepository struct {
	conn *sqlx.DB
}

// NewPostgresRepository returns a new instance of PostgresRepository
func NewPostgresRepository(dbClient *sqlx.DB) Repository {
	r := PostgresRepository{
		conn: dbClient,
	}
	var repo Repository = &r
	return repo
}

// newStatement creates a new statement builder with Dollar format bound to the connection pool.
func (r *PostgresRepository) newStatement() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(r.conn.DB)
}

// newTxStatement creates a new statement builder with Dollar format bound to a transaction.
func (r *PostgresRepository) newTxStatement(tx *sql.Tx) sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(tx)
}

// checkRowsAffected verifies that exactly nbRows were affected in a DB operation.
func (r *PostgresRepository) checkRowsAffected(res sql.Result, nbRows int64) error {
	i, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("error reading affected rows: %w", err)
	}
	if i != nbRows {
		return fmt.Errorf("expected %d row(s) affected, got %d", nbRows, i)
	}
	return nil
}

// Get retrieves an ExternalConfig by id (current version only).
func (r *PostgresRepository) Get(id int64) (ExternalConfig, bool, error) {
	rows, err := r.newStatement().
		Select("c.name", "c.folder_id", "v.data", "v.current_version", "v.created_at").
		From(table + " AS c").
		Join(versionsTable + " AS v ON c.id = v.config_id").
		Where(sq.Eq{"c.id": id, "v.current_version": true}).
		Query()
	if err != nil {
		return ExternalConfig{}, false, err
	}
	defer rows.Close()

	if rows.Next() {
		var name, data string
		var folderId *int64
		var createdAt time.Time
		var currentVersion bool
		if err := rows.Scan(&name, &folderId, &data, &currentVersion, &createdAt); err != nil {
			return ExternalConfig{}, false, fmt.Errorf("couldn't scan external config with id %d: %w", id, err)
		}
		return ExternalConfig{
			Id:             id,
			Name:           name,
			Data:           data,
			CurrentVersion: currentVersion,
			CreatedAt:      createdAt,
			FolderId:       folderId,
		}, true, nil
	}
	return ExternalConfig{}, false, nil
}

// GetByName retrieves an ExternalConfig by name (current version only).
func (r *PostgresRepository) GetByName(name string) (ExternalConfig, bool, error) {
	rows, err := r.newStatement().
		Select("c.id", "c.folder_id", "v.data", "v.current_version", "v.created_at").
		From(table + " AS c").
		Join(versionsTable + " AS v ON c.id = v.config_id").
		Where(sq.Eq{"c.name": name, "v.current_version": true}).
		Query()
	if err != nil {
		return ExternalConfig{}, false, err
	}
	defer rows.Close()

	if rows.Next() {
		var id, folderId int64
		var data string
		var createdAt time.Time
		var currentVersion bool
		if err := rows.Scan(&id, &folderId, &data, &currentVersion, &createdAt); err != nil {
			return ExternalConfig{}, false, fmt.Errorf("couldn't scan external config with name %s: %w", name, err)
		}
		return ExternalConfig{
			Id:             id,
			Name:           name,
			Data:           data,
			CurrentVersion: currentVersion,
			CreatedAt:      createdAt,
			FolderId:       &folderId,
		}, true, nil
	}
	return ExternalConfig{}, false, nil
}

// Create creates a new ExternalConfig and its first version inside a transaction.
// If externalConfig.Id is non-zero the provided id is used (useful for migrations/seeding).
func (r *PostgresRepository) Create(externalConfig ExternalConfig) (int64, error) {
	_, _, _ = utils.RefreshNextIdGen(r.conn.DB, table)

	tx, err := r.conn.Begin()
	if err != nil {
		return -1, err
	}

	stmtBuilder := r.newTxStatement(tx)

	var id int64
	var insertStmt sq.InsertBuilder
	if externalConfig.Id != 0 {
		insertStmt = stmtBuilder.
			Insert(table).
			Columns("id", "name", "folder_id").
			Values(externalConfig.Id, externalConfig.Name, externalConfig.FolderId).
			Suffix(`RETURNING "id"`)
	} else {
		insertStmt = stmtBuilder.
			Insert(table).
			Columns("name", "folder_id").
			Values(externalConfig.Name, externalConfig.FolderId).
			Suffix(`RETURNING "id"`)
	}
	if err = insertStmt.QueryRow().Scan(&id); err != nil {
		tx.Rollback()
		return -1, err
	}

	_, err = stmtBuilder.
		Insert(versionsTable).
		Columns("config_id", "data", "current_version").
		Values(id, externalConfig.Data, true).
		Exec()
	if err != nil {
		tx.Rollback()
		return -1, err
	}

	if err = tx.Commit(); err != nil {
		return -1, err
	}
	return id, nil
}

// Update updates an existing ExternalConfig, archiving the previous version.
// Returns an error if no config with the given id exists.
// Enforces a maximum version history size defined by MAX_EXTERNAL_CONFIG_VERSIONS_TO_KEEP.
// To update folder_id, please use MoveConfig
func (r *PostgresRepository) Update(id int64, externalConfig ExternalConfig) error {
	tx, err := r.conn.Begin()
	if err != nil {
		return err
	}

	stmtBuilder := r.newTxStatement(tx)

	// Update metadata and verify the row exists.
	res, err := stmtBuilder.
		Update(table).
		Set("name", externalConfig.Name).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		tx.Rollback()
		return err
	}
	if err = r.checkRowsAffected(res, 1); err != nil {
		tx.Rollback()
		return fmt.Errorf("external config with id %d not found: %w", id, err)
	}

	// Archive the current version.
	_, err = stmtBuilder.
		Update(versionsTable).
		Set("current_version", false).
		Where(sq.Eq{"config_id": id, "current_version": true}).
		Exec()
	if err != nil {
		tx.Rollback()
		return err
	}

	// Insert the new current version.
	_, err = stmtBuilder.
		Insert(versionsTable).
		Columns("config_id", "data", "current_version").
		Values(id, externalConfig.Data, true).
		Exec()
	if err != nil {
		tx.Rollback()
		return err
	}

	// Prune old versions when a limit is configured.
	maxVersions := viper.GetInt("MAX_EXTERNAL_CONFIG_VERSIONS_TO_KEEP")
	if maxVersions > 0 {
		var versionCount int
		err = stmtBuilder.
			Select("COUNT(*)").
			From(versionsTable).
			Where(sq.Eq{"config_id": id}).
			QueryRow().Scan(&versionCount)
		if err != nil {
			tx.Rollback()
			return err
		}

		if versionCount > maxVersions {
			// Use raw SQL to avoid squirrel placeholder conflicts inside the sub-query.
			_, err = tx.Exec(`
				DELETE FROM external_generic_config_versions_v1
				WHERE config_id = $1
				  AND created_at < (
				      SELECT created_at
				      FROM   external_generic_config_versions_v1
				      WHERE  config_id = $1
				      ORDER  BY created_at DESC
				      OFFSET $2
				      LIMIT  1
				  )`,
				id, maxVersions,
			)
			if err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// Delete removes an ExternalConfig (and all its versions via CASCADE) by id.
func (r *PostgresRepository) Delete(id int64) error {
	res, err := r.newStatement().
		Delete(table).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		return err
	}
	_, _, _ = utils.RefreshNextIdGen(r.conn.DB, table)
	return r.checkRowsAffected(res, 1)
}

// GetAll retrieves all ExternalConfigs (current version only).
func (r *PostgresRepository) GetAll() (map[int64]ExternalConfig, error) {
	rows, err := r.newStatement().
		Select("c.id", "c.name", "v.data", "v.current_version", "v.created_at", "c.folder_id").
		From(table + " AS c").
		Join(versionsTable + " AS v ON c.id = v.config_id").
		Where(sq.Eq{"v.current_version": true}).
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	externalConfigs := make(map[int64]ExternalConfig)
	for rows.Next() {
		var cfg ExternalConfig
		if err := rows.Scan(&cfg.Id, &cfg.Name, &cfg.Data, &cfg.CurrentVersion, &cfg.CreatedAt, &cfg.FolderId); err != nil {
			return nil, err
		}
		externalConfigs[cfg.Id] = cfg
	}
	return externalConfigs, nil
}

// GetAllOldVersions retrieves all non-current versions of an ExternalConfig, newest first.
func (r *PostgresRepository) GetAllOldVersions(id int64) ([]ExternalConfig, error) {
	rows, err := r.newStatement().
		Select("c.name", "c.folder_id", "v.data", "v.current_version", "v.created_at").
		From(table + " AS c").
		Join(versionsTable + " AS v ON c.id = v.config_id").
		Where(sq.Eq{"c.id": id, "v.current_version": false}).
		OrderBy("v.created_at DESC").
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var oldVersions []ExternalConfig
	for rows.Next() {
		var name, data string
		var folderId *int64
		var createdAt time.Time
		var currentVersion bool
		if err := rows.Scan(&name, &folderId, &data, &currentVersion, &createdAt); err != nil {
			return nil, fmt.Errorf("couldn't scan old version for config id %d: %w", id, err)
		}
		oldVersions = append(oldVersions, ExternalConfig{
			Id:             id,
			Name:           name,
			Data:           data,
			CurrentVersion: currentVersion,
			CreatedAt:      createdAt,
			FolderId:       folderId,
		})
	}
	return oldVersions, nil
}

// MoveConfig moves an ExternalConfig to a different folder.
// Pass nil for newFolderId to move it to the root level.
// Returns an error if the config or the target folder does not exist.
func (r *PostgresRepository) MoveConfig(id int64, newFolderId *int64) error {
	if newFolderId != nil {
		_, found, err := r.GetFolder(*newFolderId)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("destination folder with id %d not found", *newFolderId)
		}
	}

	res, err := r.newStatement().
		Update(table).
		Set("folder_id", newFolderId).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		return err
	}
	return r.checkRowsAffected(res, 1)
}

// CreateFolder creates a new folder.
func (r *PostgresRepository) CreateFolder(folder Folder) (int64, error) {
	_, _, _ = utils.RefreshNextIdGen(r.conn.DB, foldersTable)

	var id int64
	err := r.newStatement().
		Insert(foldersTable).
		Columns("name", "parent_id").
		Values(folder.Name, folder.ParentId).
		Suffix(`RETURNING "id"`).
		QueryRow().Scan(&id)
	if err != nil {
		return -1, err
	}
	return id, nil
}

// GetFolder retrieves a folder by id.
func (r *PostgresRepository) GetFolder(id int64) (Folder, bool, error) {
	rows, err := r.newStatement().
		Select("id", "name", "parent_id", "created_at", "updated_at").
		From(foldersTable).
		Where(sq.Eq{"id": id}).
		Query()
	if err != nil {
		return Folder{}, false, err
	}
	defer rows.Close()

	if rows.Next() {
		var folder Folder
		if err := rows.Scan(&folder.Id, &folder.Name, &folder.ParentId, &folder.CreatedAt, &folder.UpdatedAt); err != nil {
			return Folder{}, false, fmt.Errorf("couldn't scan folder with id %d: %w", id, err)
		}
		return folder, true, nil
	}
	return Folder{}, false, nil
}

// GetFolderByName retrieves a folder by name and optional parent_id.
func (r *PostgresRepository) GetFolderByName(name string, parentId *int64) (Folder, bool, error) {
	query := r.newStatement().
		Select("id", "name", "parent_id", "created_at", "updated_at").
		From(foldersTable).
		Where(sq.Eq{"name": name})

	if parentId == nil {
		query = query.Where("parent_id IS NULL")
	} else {
		query = query.Where(sq.Eq{"parent_id": *parentId})
	}

	rows, err := query.Query()
	if err != nil {
		return Folder{}, false, err
	}
	defer rows.Close()

	if rows.Next() {
		var folder Folder
		if err := rows.Scan(&folder.Id, &folder.Name, &folder.ParentId, &folder.CreatedAt, &folder.UpdatedAt); err != nil {
			return Folder{}, false, fmt.Errorf("couldn't scan folder with name %s: %w", name, err)
		}
		return folder, true, nil
	}
	return Folder{}, false, nil
}

// UpdateFolder updates a folder's name and parent_id.
func (r *PostgresRepository) UpdateFolder(id int64, folder Folder) error {
	res, err := r.newStatement().
		Update(foldersTable).
		Set("name", folder.Name).
		Set("parent_id", folder.ParentId).
		Set("updated_at", time.Now()).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		return err
	}
	return r.checkRowsAffected(res, 1)
}

// MoveFolder moves a folder to a new parent.
// Pass nil for newParentId to place it at the root level.
// Returns an error if:
//   - the folder does not exist,
//   - the target parent does not exist, or
//   - the move would create a circular reference.
func (r *PostgresRepository) MoveFolder(id int64, newParentId *int64) error {
	if newParentId != nil && *newParentId == id {
		return fmt.Errorf("cannot move folder %d: a folder cannot be its own parent", id)
	}

	// Verify the source folder exists.
	_, found, err := r.GetFolder(id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("folder with id %d not found", id)
	}

	if newParentId != nil {
		// Guard against circular references: newParentId must not be a descendant of id.
		isDescendant, err := r.isFolderDescendant(id, *newParentId)
		if err != nil {
			return err
		}
		if isDescendant {
			return fmt.Errorf(
				"cannot move folder %d under folder %d: would create a circular reference",
				id, *newParentId,
			)
		}

		// Verify the target parent exists.
		_, found, err = r.GetFolder(*newParentId)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("destination folder with id %d not found", *newParentId)
		}
	}

	res, err := r.newStatement().
		Update(foldersTable).
		Set("parent_id", newParentId).
		Set("updated_at", time.Now()).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		return err
	}
	return r.checkRowsAffected(res, 1)
}

// isFolderDescendant reports whether candidateId is equal to or a descendant of ancestorId,
// using a recursive CTE for efficient traversal.
func (r *PostgresRepository) isFolderDescendant(ancestorId, candidateId int64) (bool, error) {
	const query = `
		WITH RECURSIVE descendants AS (
			SELECT id
			FROM   external_config_folders_v1
			WHERE  id = $1
			UNION ALL
			SELECT f.id
			FROM   external_config_folders_v1 f
			JOIN   descendants d ON f.parent_id = d.id
		)
		SELECT EXISTS(SELECT 1 FROM descendants WHERE id = $2)
	`
	var isDescendant bool
	if err := r.conn.QueryRow(query, ancestorId, candidateId).Scan(&isDescendant); err != nil {
		return false, err
	}
	return isDescendant, nil
}

// DeleteFolder deletes a folder by id.
// Configs inside the folder will have folder_id set to NULL (requires FK ON DELETE SET NULL).
func (r *PostgresRepository) DeleteFolder(id int64) error {
	res, err := r.newStatement().
		Delete(foldersTable).
		Where(sq.Eq{"id": id}).
		Exec()
	if err != nil {
		return err
	}
	_, _, _ = utils.RefreshNextIdGen(r.conn.DB, foldersTable)
	return r.checkRowsAffected(res, 1)
}

// GetAllFolders retrieves all folders ordered by parent_id then name.
func (r *PostgresRepository) GetAllFolders() (map[int64]Folder, error) {
	rows, err := r.newStatement().
		Select("id", "name", "parent_id", "created_at", "updated_at").
		From(foldersTable).
		OrderBy("parent_id", "name").
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	folders := make(map[int64]Folder)
	for rows.Next() {
		var folder Folder
		if err := rows.Scan(&folder.Id, &folder.Name, &folder.ParentId, &folder.CreatedAt, &folder.UpdatedAt); err != nil {
			return nil, err
		}
		folders[folder.Id] = folder
	}
	return folders, nil
}

// GetFolderHierarchy returns the direct child folders and the configs that live
// directly at the requested level (parentId == nil → root).
// "Root" means all folders with parent_id = NULL and all configs with folder_id = NULL.
// All folders and current-version configs are fetched in two queries; the tree is built in memory.
func (r *PostgresRepository) GetFolderHierarchy(parentId *int64) (HierarchyResult, error) {
	allFolders, err := r.GetAllFolders()
	if err != nil {
		return HierarchyResult{}, err
	}

	allConfigs, err := r.getAllConfigs()
	if err != nil {
		return HierarchyResult{}, err
	}

	return HierarchyResult{
		Folders: r.buildFolderTree(allFolders, allConfigs, parentId),
		Configs: r.getConfigsForFolder(parentId, allConfigs),
	}, nil
}

// buildFolderTree recursively builds a FolderNode tree from flat lookup data.
func (r *PostgresRepository) buildFolderTree(allFolders map[int64]Folder, allConfigs []ExternalConfig, parentId *int64) []FolderNode {
	var nodes []FolderNode
	for _, folder := range allFolders {
		sameLevel := (parentId == nil && folder.ParentId == nil) ||
			(parentId != nil && folder.ParentId != nil && *folder.ParentId == *parentId)
		if !sameLevel {
			continue
		}
		nodes = append(nodes, FolderNode{
			Folder:     folder,
			SubFolders: r.buildFolderTree(allFolders, allConfigs, &folder.Id),
			Configs:    r.getConfigsForFolder(&folder.Id, allConfigs),
		})
	}
	return nodes
}

// getConfigsForFolder filters configs that belong to the specified folder.
func (r *PostgresRepository) getConfigsForFolder(folderId *int64, allConfigs []ExternalConfig) []ExternalConfig {
	var configs []ExternalConfig
	for _, config := range allConfigs {
		sameFolder := (folderId == nil && config.FolderId == nil) ||
			(folderId != nil && config.FolderId != nil && *config.FolderId == *folderId)
		if sameFolder {
			configs = append(configs, config)
		}
	}
	return configs
}

// getAllConfigs retrieves all current-version configs (used internally for tree building).
func (r *PostgresRepository) getAllConfigs() ([]ExternalConfig, error) {
	rows, err := r.newStatement().
		Select("c.id", "c.name", "v.data", "v.current_version", "v.created_at", "c.folder_id").
		From(table + " AS c").
		Join(versionsTable + " AS v ON c.id = v.config_id").
		Where(sq.Eq{"v.current_version": true}).
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []ExternalConfig
	for rows.Next() {
		var cfg ExternalConfig
		if err := rows.Scan(&cfg.Id, &cfg.Name, &cfg.Data, &cfg.CurrentVersion, &cfg.CreatedAt, &cfg.FolderId); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

// GetFolderParentCount returns the depth of a folder in the hierarchy (0 = root level).
// Uses a recursive CTE to traverse up the ancestor chain efficiently.
func (r *PostgresRepository) GetFolderParentCount(folderId int64) (int, error) {
	const query = `
		WITH RECURSIVE parent_chain AS (
			SELECT id, parent_id, 0 AS depth
			FROM   external_config_folders_v1
			WHERE  id = $1
			UNION ALL
			SELECT f.id, f.parent_id, pc.depth + 1
			FROM   external_config_folders_v1 f
			JOIN   parent_chain pc ON f.id = pc.parent_id
		)
		SELECT COALESCE(MAX(depth), 0) AS parent_count
		FROM   parent_chain
		WHERE  parent_id IS NOT NULL
	`
	var count int
	err := r.conn.QueryRow(query, folderId).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("folder with id %d not found", folderId)
		}
		return 0, err
	}

	return count, nil
}
