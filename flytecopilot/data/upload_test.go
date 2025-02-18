package data

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/flyteorg/flyte/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyte/flytestdlib/promutils"
	"github.com/flyteorg/flyte/flytestdlib/storage"
)

func TestUploader_RecursiveUpload(t *testing.T) {

	tmpFolderLocation := ""
	tmpPrefix := "upload_test"

	t.Run("upload-blob", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp(tmpFolderLocation, tmpPrefix)
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, os.RemoveAll(tmpDir))
		}()

		vmap := &core.VariableMap{
			Variables: map[string]*core.Variable{
				"x": {
					Type: &core.LiteralType{Type: &core.LiteralType_Blob{Blob: &core.BlobType{Dimensionality: core.BlobType_SINGLE}}},
				},
				"y": {
					Type: &core.LiteralType{Type: &core.LiteralType_Blob{Blob: &core.BlobType{Dimensionality: core.BlobType_MULTIPART}}},
				},
			},
		}

		data := []byte("data")
		assert.NoError(t, os.WriteFile(path.Join(tmpDir, "x"), data, os.ModePerm)) // #nosec G306
		fmt.Printf("Written to %s ", path.Join(tmpDir, "x"))

		// Create directory y with multiple files inside
		yDir := path.Join(tmpDir, "y")
		assert.NoError(t, os.Mkdir(yDir, os.ModePerm))
		assert.NoError(t, os.WriteFile(path.Join(yDir, "file1.txt"), []byte("dir data 1"), os.ModePerm))
		assert.NoError(t, os.WriteFile(path.Join(yDir, "file2.txt"), []byte("dir data 2"), os.ModePerm))
		assert.NoError(t, os.WriteFile(path.Join(yDir, "file3.json"), []byte("{\"key\": \"value\"}"), os.ModePerm))

		store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
		assert.NoError(t, err)

		outputRef := storage.DataReference("output")
		rawRef := storage.DataReference("raw")
		u := NewUploader(context.TODO(), store, core.DataLoadingConfig_JSON, core.IOStrategy_UPLOAD_ON_EXIT, "error")
		assert.NoError(t, u.RecursiveUpload(context.TODO(), vmap, tmpDir, outputRef, rawRef))

		outputs := &core.LiteralMap{}
		assert.NoError(t, store.ReadProtobuf(context.TODO(), outputRef, outputs))
		assert.Len(t, outputs.GetLiterals(), 2)

		// Check single file blob
		assert.NotNil(t, outputs.GetLiterals()["x"])
		assert.NotNil(t, outputs.GetLiterals()["x"].GetScalar())
		assert.NotNil(t, outputs.GetLiterals()["x"].GetScalar().GetBlob())
		ref := storage.DataReference(outputs.GetLiterals()["x"].GetScalar().GetBlob().GetUri())
		r, err := store.ReadRaw(context.TODO(), ref)
		assert.NoError(t, err, "%s does not exist", ref)
		defer r.Close()
		b, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(b), "content dont match")

		// Check directory blob
		assert.NotNil(t, outputs.GetLiterals()["y"])
		assert.NotNil(t, outputs.GetLiterals()["y"].GetScalar())
		assert.NotNil(t, outputs.GetLiterals()["y"].GetScalar().GetBlob())
		assert.Equal(t, core.BlobType_MULTIPART, outputs.GetLiterals()["y"].GetScalar().GetBlob().GetMetadata().GetType().GetDimensionality())
		dirRef := storage.DataReference(outputs.GetLiterals()["y"].GetScalar().GetBlob().GetUri())

		// Check file1.txt
		fileRef1, err := store.ConstructReference(context.TODO(), dirRef, "file1.txt")
		assert.NoError(t, err)
		r1, err := store.ReadRaw(context.TODO(), fileRef1)
		assert.NoError(t, err)
		defer r1.Close()
		b1, err := io.ReadAll(r1)
		assert.NoError(t, err)
		assert.Equal(t, "dir data 1", string(b1))

		// Check file2.txt
		fileRef2, err := store.ConstructReference(context.TODO(), dirRef, "file2.txt")
		assert.NoError(t, err)
		r2, err := store.ReadRaw(context.TODO(), fileRef2)
		assert.NoError(t, err)
		defer r2.Close()
		b2, err := io.ReadAll(r2)
		assert.NoError(t, err)
		assert.Equal(t, "dir data 2", string(b2))

		// Check file3.json
		fileRef3, err := store.ConstructReference(context.TODO(), dirRef, "file3.json")
		assert.NoError(t, err)
		r3, err := store.ReadRaw(context.TODO(), fileRef3)
		assert.NoError(t, err)
		defer r3.Close()
		b3, err := io.ReadAll(r3)
		assert.NoError(t, err)
		assert.Equal(t, "{\"key\": \"value\"}", string(b3))
	})
}

func TestUploader_handleBlobType_Directory(t *testing.T) {
	// Setup a temporary directory with a file
	tempDir, err := os.MkdirTemp("", "uploader_test")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	filePath := filepath.Join(tempDir, "file.txt")
	err = os.WriteFile(filePath, []byte("content"), 0644)
	assert.NoError(t, err)

	// Create a mock data store
	store, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	assert.NoError(t, err)

	uploader := NewUploader(context.Background(), store, core.DataLoadingConfig_JSON, core.IOStrategy_UPLOAD_ON_EXIT, "error.log")

	// Perform the upload
	toPath := storage.DataReference("s3://test-bucket/")
	literal, err := uploader.handleBlobType(context.Background(), tempDir, toPath)
	assert.NoError(t, err)

	// Check that the output is a blob and the directory flag is set
	assert.NotNil(t, literal.GetScalar().GetBlob())
	assert.Equal(t, core.BlobType_MULTIPART, literal.GetScalar().GetBlob().Metadata.Type.Dimensionality)
	assert.Equal(t, "s3://test-bucket/", literal.GetScalar().GetBlob().Uri)
}
