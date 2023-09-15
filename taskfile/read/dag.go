package read

import (
	"fmt"
	"os"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
	"golang.org/x/sync/errgroup"

	"github.com/go-task/task/v3/internal/filepathext"
	"github.com/go-task/task/v3/taskfile"
)

type TaskfileGraph struct {
	graph.Graph[string, *TaskfileVertex]
}

// A TaskfileVertex is a vertex on the Taskfile DAG.
type TaskfileVertex struct {
	uri      string
	taskfile *taskfile.Taskfile
}

func taskfileHash(vertex *TaskfileVertex) string {
	return vertex.uri
}

func NewTaskfileGraph() *TaskfileGraph {
	return &TaskfileGraph{
		graph.New(taskfileHash,
			graph.Directed(),
			graph.PreventCycles(),
			graph.Rooted(),
		),
	}
}

func (r *TaskfileGraph) Visualize(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return draw.DOT(r.Graph, f)
}

func (dag *TaskfileGraph) Merge() (*taskfile.Taskfile, error) {
	hashes, err := graph.TopologicalSort(dag.Graph)
	if err != nil {
		return nil, err
	}

	predecessorMap, err := dag.PredecessorMap()
	if err != nil {
		return nil, err
	}

	for i := len(hashes) - 1; i >= 0; i-- {
		hash := hashes[i]

		// Get the current vertex
		vertex, err := dag.Vertex(hash)
		if err != nil {
			return nil, err
		}

		// Create an error group to wait for all the included Taskfiles to be merged with all its parents
		var g errgroup.Group

		// Loop over each adjacent edge
		for _, edge := range predecessorMap[hash] {

			// TODO: Enable goroutines
			// Start a goroutine to process each included Taskfile
			// g.Go(
			err := func() error {
				// Get the child vertex
				predecessorVertex, err := dag.Vertex(edge.Source)
				if err != nil {
					return err
				}

				// Get the merge options
				mergeOptions, ok := edge.Properties.Data.(*taskfile.MergeOptions)
				if !ok {
					return fmt.Errorf("task: Failed to get merge options")
				}

				// Handle advanced imports
				// i.e. where additional data is given when a Taskfile is included
				if mergeOptions.AdvancedImport {
					vertex.taskfile.Vars.Range(func(k string, v taskfile.Var) error {
						o := v
						o.Dir = mergeOptions.Dir
						vertex.taskfile.Vars.Set(k, o)
						return nil
					})
					vertex.taskfile.Env.Range(func(k string, v taskfile.Var) error {
						o := v
						o.Dir = mergeOptions.Dir
						vertex.taskfile.Env.Set(k, o)
						return nil
					})
					for _, task := range vertex.taskfile.Tasks.Values() {
						task.Dir = filepathext.SmartJoin(mergeOptions.Dir, task.Dir)
						if task.IncludeVars == nil {
							task.IncludeVars = &taskfile.Vars{}
						}
						task.IncludeVars.Merge(mergeOptions.Vars)
						task.IncludedTaskfileVars = vertex.taskfile.Vars
						task.IncludedTaskfileDir = mergeOptions.Dir
					}
				}

				// Merge the included Taskfile into the parent Taskfile
				if err := taskfile.Merge(
					predecessorVertex.taskfile,
					vertex.taskfile,
					mergeOptions,
				); err != nil {
					return err
				}

				return nil
			}()
			if err != nil {
				return nil, err
			}
			// )
		}

		// Wait for all the go routines to finish
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}

	// Get the root vertex
	rootVertex, err := dag.Vertex(hashes[0])
	if err != nil {
		return nil, err
	}

	rootVertex.taskfile.Tasks.Range(func(name string, task *taskfile.Task) error {
		if task == nil {
			task = &taskfile.Task{}
			rootVertex.taskfile.Tasks.Set(name, task)
		}
		task.Task = name
		return nil
	})

	return rootVertex.taskfile, nil
}
