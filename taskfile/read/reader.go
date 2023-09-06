package read

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dominikbraun/graph"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/go-task/task/v3/errors"
	"github.com/go-task/task/v3/internal/execext"
	"github.com/go-task/task/v3/internal/filepathext"
	"github.com/go-task/task/v3/internal/logger"
	"github.com/go-task/task/v3/internal/templater"
	"github.com/go-task/task/v3/taskfile"
)

// A Reader will recursively read Taskfiles from a given source using a directed
// acyclic graph (DAG).
type Reader struct {
	graph    *TaskfileGraph
	uri      string
	insecure bool
	download bool
	offline  bool
	tempDir  string
	logger   *logger.Logger
}

func NewReader(
	uri string,
	insecure bool,
	download bool,
	offline bool,
	tempDir string,
	logger *logger.Logger,
) *Reader {
	return &Reader{
		graph:    NewTaskfileGraph(),
		uri:      uri,
		insecure: insecure,
		download: download,
		offline:  offline,
		tempDir:  tempDir,
		logger:   logger,
	}
}

func (r *Reader) Read() (*TaskfileGraph, error) {
	// Create a new reader node
	node, err := NewNode(r.uri, r.insecure)
	if err != nil {
		return nil, err
	}

	// Recursively loop through each Taskfile, adding vertices/edges to the graph
	if err := r.addIncludedTaskfiles(node); err != nil {
		return nil, err
	}

	return r.graph, nil
}

func (r *Reader) addIncludedTaskfiles(node Node) error {
	// Create a new vertex for the Taskfile
	vertex := &TaskfileVertex{
		uri:      node.Location(),
		taskfile: nil,
	}

	// Add the included Taskfile to the DAG
	// If the vertex already exists, we return early since its Taskfile has
	// already been read and its children explored
	if err := r.graph.AddVertex(vertex); err == graph.ErrVertexAlreadyExists {
		return nil
	} else if err != nil {
		return err
	}

	// Read and parse the Taskfile from the file and add it to the vertex
	var err error
	vertex.taskfile, err = r.readNode(node)
	if err != nil {
		if node.Optional() {
			return nil
		}
		return err
	}

	// TODO: Maybe move this somewhere else?
	// Annotate any included Taskfile reference with a base directory for resolving relative paths
	if node, isFileNode := node.(*FileNode); isFileNode {
		_ = vertex.taskfile.Includes.Range(func(namespace string, includedFile taskfile.IncludedTaskfile) error {
			// Set the base directory for resolving relative paths, but only if not already set
			if includedFile.BaseDir == "" {
				includedFile.BaseDir = node.Dir
				vertex.taskfile.Includes.Set(namespace, includedFile)
			}
			return nil
		})
	}

	// Create an error group to wait for all included Taskfiles to be read
	var g errgroup.Group

	// Loop over each included taskfile
	vertex.taskfile.Includes.Range(func(namespace string, includedTaskfile taskfile.IncludedTaskfile) error {
		// Start a goroutine to process each included Taskfile
		g.Go(func() error {
			// If the Taskfile schema is v3 or higher, replace all variables with their values
			if vertex.taskfile.Version.Compare(taskfile.V3) >= 0 {
				tr := templater.Templater{Vars: vertex.taskfile.Vars, RemoveNoValue: true}
				includedTaskfile.Taskfile = tr.Replace(includedTaskfile.Taskfile)
				includedTaskfile.Dir = tr.Replace(includedTaskfile.Dir)
				if err := tr.Err(); err != nil {
					return err
				}
			}

			uri, err := includedTaskfile.FullTaskfilePath()
			if err != nil {
				return err
			}

			includedTaskfileNode, err := NewNode(uri, r.insecure,
				WithParent(node),
				WithOptional(includedTaskfile.Optional),
			)
			if err != nil {
				return err
			}

			// Recurse into the included Taskfile
			if err := r.addIncludedTaskfiles(includedTaskfileNode); err != nil {
				return err
			}

			mergeOptions := &taskfile.MergeOptions{
				Namespace:      namespace,
				Dir:            includedTaskfile.Dir,
				Internal:       includedTaskfile.Internal,
				Aliases:        includedTaskfile.Aliases,
				Vars:           includedTaskfile.Vars,
				AdvancedImport: includedTaskfile.AdvancedImport,
			}

			// Create an edge between the Taskfiles
			err = r.graph.AddEdge(node.Location(), includedTaskfileNode.Location(), graph.EdgeData(mergeOptions))
			if errors.Is(err, graph.ErrEdgeAlreadyExists) {
				edge, err := r.graph.Edge(node.Location(), includedTaskfileNode.Location())
				if err != nil {
					return err
				}
				return &errors.TaskfileDuplicateIncludeError{
					URI:         node.Location(),
					IncludedURI: includedTaskfileNode.Location(),
					Namespaces:  []string{namespace, edge.Properties.Data.(*taskfile.MergeOptions).Namespace},
				}
			}
			if errors.Is(err, graph.ErrEdgeCreatesCycle) {
				return errors.TaskfileCycleError{
					Source:      node.Location(),
					Destination: includedTaskfileNode.Location(),
				}
			}
			return err
		})
		return nil
	})

	// Wait for all the go routines to finish
	return g.Wait()
}

func (r *Reader) readNode(node Node) (*taskfile.Taskfile, error) {
	var b []byte
	var err error
	var cache *Cache

	if node.Remote() {
		cache, err = NewCache(r.tempDir)
		if err != nil {
			return nil, err
		}
	}

	// If the file is remote, check if we have a cached copy
	// If we're told to download, skip the cache
	if node.Remote() && !r.download {
		if b, err = cache.read(node); !errors.Is(err, os.ErrNotExist) && err != nil {
			return nil, err
		}

		if b != nil {
			r.logger.VerboseOutf(logger.Magenta, "task: [%s] Fetched cached copy\n", node.Location())
		}
	}

	// If the file is remote, we found nothing in the cache and we're not
	// allowed to download it then we can't do anything.
	if node.Remote() && b == nil && r.offline {
		if b == nil && r.offline {
			return nil, &errors.TaskfileCacheNotFound{URI: node.Location()}
		}
	}

	// If we still don't have a copy, get the file in the usual way
	if b == nil {
		b, err = node.Read(context.Background())
		if err != nil {
			return nil, err
		}

		// If the node was remote, we need to check the checksum
		if node.Remote() {
			r.logger.VerboseOutf(logger.Magenta, "task: [%s] Fetched remote copy\n", node.Location())

			// Get the checksums
			checksum := checksum(b)
			cachedChecksum := cache.readChecksum(node)

			// If the checksum doesn't exist, prompt the user to continue
			if cachedChecksum == "" {
				if cont, err := r.logger.Prompt(logger.Yellow, fmt.Sprintf("The task you are attempting to run depends on the remote Taskfile at %q.\n--- Make sure you trust the source of this Taskfile before continuing ---\nContinue?", node.Location()), "n", "y", "yes"); err != nil {
					return nil, err
				} else if !cont {
					return nil, &errors.TaskfileNotTrustedError{URI: node.Location()}
				}
			} else if checksum != cachedChecksum {
				// If there is a cached hash, but it doesn't match the expected hash, prompt the user to continue
				if cont, err := r.logger.Prompt(logger.Yellow, fmt.Sprintf("The Taskfile at %q has changed since you last used it!\n--- Make sure you trust the source of this Taskfile before continuing ---\nContinue?", node.Location()), "n", "y", "yes"); err != nil {
					return nil, err
				} else if !cont {
					return nil, &errors.TaskfileNotTrustedError{URI: node.Location()}
				}
			}

			// If the hash has changed (or is new), store it in the cache
			if checksum != cachedChecksum {
				if err := cache.writeChecksum(node, checksum); err != nil {
					return nil, err
				}
			}
		}
	}

	// If the file is remote and we need to cache it
	if node.Remote() && r.download {
		r.logger.VerboseOutf(logger.Magenta, "task: [%s] Caching downloaded file\n", node.Location())
		// Cache the file for later
		if err = cache.write(node, b); err != nil {
			return nil, err
		}
	}

	var t taskfile.Taskfile
	if err := yaml.Unmarshal(b, &t); err != nil {
		return nil, &errors.TaskfileInvalidError{URI: filepathext.TryAbsToRel(node.Location()), Err: err}
	}
	t.Location = node.Location()

	return &t, nil
}

func resolvePath(baseDir, path string) (string, error) {
	path, err := execext.Expand(path)
	if err != nil {
		return "", err
	}

	if filepath.IsAbs(path) {
		return path, nil
	}

	result, err := filepath.Abs(filepathext.SmartJoin(baseDir, path))
	if err != nil {
		return "", fmt.Errorf("task: error resolving path %s relative to %s: %w", path, baseDir, err)
	}

	return result, nil
}
