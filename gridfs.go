// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package blobstore

import (
	"io"

	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/mgo.v2"
)

var logger = loggo.GetLogger("juju.storage")

type gridFSStorage struct {
	dbName    string
	namespace string
	session   *mgo.Session
}

var _ ResourceStorage = (*gridFSStorage)(nil)

// NewGridFS returns a ResourceStorage instance backed by a mongo GridFS.
// namespace is used to segregate different sets of data.
func NewGridFS(dbName, namespace string, session *mgo.Session) ResourceStorage {
	return &gridFSStorage{
		dbName:    dbName,
		namespace: namespace,
		session:   session,
	}
}

func (g *gridFSStorage) db() *mgo.Database {
	s := g.session.Copy()
	return s.DB(g.dbName)
}

func (g *gridFSStorage) gridFS() *gridFS {
	db := g.db()
	return &gridFS{
		GridFS:  db.GridFS(g.namespace),
		session: db.Session,
	}
}

// gridFS wraps a GridFS so that the session can be closed when finished
// with.
type gridFS struct {
	*mgo.GridFS
	session *mgo.Session
}

func (g *gridFS) Close() {
	g.session.Close()
}

// gridfsFile wraps a GridFile so that the session can be closed when finished
// with.
type gridfsFile struct {
	*mgo.GridFile
	gfs *gridFS
}

func (f *gridfsFile) Close() error {
	defer f.gfs.Close()
	return f.GridFile.Close()
}

// Get is defined on ResourceStorage.
func (g *gridFSStorage) Get(path string) (io.ReadCloser, error) {
	gfs := g.gridFS()
	file, err := gfs.Open(path)
	if err != nil {
		gfs.Close()
		return nil, errors.Annotatef(err, "failed to open GridFS file %q", path)
	}
	return &gridfsFile{
		GridFile: file,
		gfs:      gfs,
	}, nil
}

// Put is defined on ResourceStorage.
func (g *gridFSStorage) Put(path string, r io.Reader, length int64) (checksum string, err error) {
	gfs := g.gridFS()
	defer gfs.Close()
	file, err := gfs.Create(path)
	if err != nil {
		return "", errors.Annotatef(err, "failed to create GridFS file %q", path)
	}
	defer func() {
		if err != nil {
			file.Close()
			if removeErr := g.Remove(path); removeErr != nil {
				logger.Warningf("error cleaning up after failed write: %v", removeErr)
			}
		}
	}()
	if _, err = io.CopyN(file, r, length); err != nil {
		return "", errors.Annotatef(err, "failed to write data")
	}
	if err = file.Close(); err != nil {
		return "", errors.Annotatef(err, "failed to flush data")
	}
	return file.MD5(), nil
}

// Remove is defined on ResourceStorage.
func (g *gridFSStorage) Remove(path string) error {
	gfs := g.gridFS()
	defer gfs.Close()
	return gfs.Remove(path)
}
