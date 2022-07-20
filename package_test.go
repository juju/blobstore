// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package blobstore_test

import (
	"testing"

	mgotesting "github.com/juju/mgo/v3/testing"
)

func Test(t *testing.T) {
	mgotesting.MgoTestPackage(t, nil)
}
