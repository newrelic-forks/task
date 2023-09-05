package taskfile

import (
	"errors"
	"fmt"
	"strings"
)

type MergeOptions struct {
	Namespace string
	Dir       string
	Internal  bool
	Aliases   []string
	Vars      *Vars
}

// ErrIncludedTaskfilesCantHaveDotenvs is returned when a included Taskfile contains dotenvs
var ErrIncludedTaskfilesCantHaveDotenvs = errors.New("task: Included Taskfiles can't have dotenv declarations. Please, move the dotenv declaration to the main Taskfile")

// NamespaceSeparator contains the character that separates namespaces
const NamespaceSeparator = ":"

// Merge merges the second Taskfile into the first
func Merge(t1, t2 *Taskfile, opts *MergeOptions) error {
	if !t1.Version.Equal(t2.Version) {
		return fmt.Errorf(`task: Taskfiles versions should match. First is "%s" but second is "%s"`, t1.Version, t2.Version)
	}
	if t1.Version.Compare(V3) >= 0 && len(t2.Dotenv) > 0 {
		return ErrIncludedTaskfilesCantHaveDotenvs
	}
	if t2.Expansions != 0 && t2.Expansions != 2 {
		t1.Expansions = t2.Expansions
	}
	if t2.Output.IsSet() {
		t1.Output = t2.Output
	}
	if t1.Vars == nil {
		t1.Vars = &Vars{}
	}
	if t1.Env == nil {
		t1.Env = &Vars{}
	}
	t1.Vars.Merge(t2.Vars)
	t1.Env.Merge(t2.Env)

	return t2.Tasks.Range(func(k string, v *Task) error {
		// We do a deep copy of the task struct here to ensure that no data can
		// be changed elsewhere once the taskfile is merged.
		task := v.DeepCopy()

		// Set the task to internal if EITHER the included task or the included
		// taskfile are marked as internal
		task.Internal = task.Internal || (opts != nil && opts.Internal)

		// Add namespaces to task dependencies
		for _, dep := range task.Deps {
			dep.Task = taskNameWithNamespace(dep.Task, opts.Namespace)
		}

		// Add namespaces to task commands
		for _, cmd := range task.Cmds {
			if cmd != nil && cmd.Task != "" {
				cmd.Task = taskNameWithNamespace(cmd.Task, opts.Namespace)
			}
		}

		// Add namespaces to task aliases
		for i, alias := range task.Aliases {
			task.Aliases[i] = taskNameWithNamespace(alias, opts.Namespace)
		}

		// Add namespace aliases
		if opts != nil {
			for _, namespaceAlias := range opts.Aliases {
				task.Aliases = append(task.Aliases, taskNameWithNamespace(task.Task, namespaceAlias))
				for _, alias := range v.Aliases {
					task.Aliases = append(task.Aliases, taskNameWithNamespace(alias, namespaceAlias))
				}
			}
		}

		// Add the task to the merged taskfile
		taskNameWithNamespace := taskNameWithNamespace(k, opts.Namespace)
		task.Task = taskNameWithNamespace
		t1.Tasks.Set(taskNameWithNamespace, task)

		// If the included Taskfile has a default task and the parent namespace has
		// no task with a matching name, we can add an alias so that the user can
		// run the included Taskfile's default task without specifying its full
		// name. If the parent namespace has aliases, we add another alias for each
		// of them.
		if t2.Tasks.Get("default") != nil && t1.Tasks.Get(opts.Namespace) == nil {
			defaultTaskName := fmt.Sprintf("%s:default", opts.Namespace)
			t1.Tasks.Get(defaultTaskName).Aliases = append(t1.Tasks.Get(defaultTaskName).Aliases, opts.Namespace)
			t1.Tasks.Get(defaultTaskName).Aliases = append(t1.Tasks.Get(defaultTaskName).Aliases, opts.Aliases...)
		}

		return nil
	})
}

func taskNameWithNamespace(taskName string, namespace string) string {
	if strings.HasPrefix(taskName, ":") {
		return strings.TrimPrefix(taskName, ":")
	}
	return fmt.Sprintf("%s%s%s", namespace, NamespaceSeparator, taskName)
}
