package read

import (
	"os"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"

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
