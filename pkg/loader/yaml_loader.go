package loader

import (
	"fmt"

	"github.com/tcmartin/flowlib"
	"github.com/tcmartin/flowrunner/pkg/plugins"
	"gopkg.in/yaml.v2"
)

// DefaultYAMLLoader implements the YAMLLoader interface
type DefaultYAMLLoader struct {
	nodeFactories    map[string]plugins.NodeFactory
	pluginRegistry plugins.PluginRegistry
}

// NewYAMLLoader creates a new YAML loader
func NewYAMLLoader(nodeFactories map[string]plugins.NodeFactory, pluginRegistry plugins.PluginRegistry) YAMLLoader {
	return &DefaultYAMLLoader{
		nodeFactories:    nodeFactories,
		pluginRegistry: pluginRegistry,
	}
}

// Parse converts a YAML string into a Flowlib graph
func (l *DefaultYAMLLoader) Parse(yamlContent string) (*flowlib.Flow, error) {
	// First validate the YAML
	if err := l.Validate(yamlContent); err != nil {
		return nil, err
	}

	var flowDef FlowDefinition
	if err := yaml.Unmarshal([]byte(yamlContent), &flowDef); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

    // Create all the nodes
    nodes := make(map[string]flowlib.Node)
    for nodeName, nodeDef := range flowDef.Nodes {
        // Inject node_id into params for downstream logging/diagnostics
        if nodeDef.Params == nil {
            nodeDef.Params = make(map[string]interface{})
        }
        // Do not override if already present
        if _, has := nodeDef.Params["node_id"]; !has {
            nodeDef.Params["node_id"] = nodeName
        }

        factory, exists := l.nodeFactories[nodeDef.Type]
		if !exists {
			// If the node type is not in the built-in factories, try the plugin registry
			plugin, err := l.pluginRegistry.Get(nodeDef.Type)
			if err != nil {
				return nil, fmt.Errorf("unknown node type '%s' in node '%s'", nodeDef.Type, nodeName)
			}
			nodePlugin, ok := plugin.(plugins.NodePlugin)
			if !ok {
				return nil, fmt.Errorf("plugin '%s' is not a valid NodePlugin", nodeDef.Type)
			}
			node, err := nodePlugin.CreateNode(nodeDef.Params)
			if err != nil {
				return nil, fmt.Errorf("failed to create node '%s' from plugin: %w", nodeName, err)
			}
			nodes[nodeName] = node
		} else {
            node, err := factory.CreateNode(nodeDef)
			if err != nil {
				return nil, fmt.Errorf("failed to create node '%s': %w", nodeName, err)
			}
			nodes[nodeName] = node
		}
	}

	// Connect the nodes
	for nodeName, nodeDef := range flowDef.Nodes {
		node := nodes[nodeName]
		for action, nextNodeName := range nodeDef.Next {
			nextNode, exists := nodes[nextNodeName]
			if !exists {
				return nil, fmt.Errorf("node '%s' references non-existent node '%s' for action '%s'", nodeName, nextNodeName, action)
			}
			node.Next(flowlib.Action(action), nextNode)
		}
	}

	// Find the start node (the one not referenced by any other node)
	startNode, err := findStartNode(flowDef, nodes)
	if err != nil {
		return nil, err
	}

	return flowlib.NewFlow(startNode), nil
}

// Validate checks if a YAML string conforms to the schema
func (l *DefaultYAMLLoader) Validate(yamlContent string) error {
	// Basic YAML validation
	var flowDef FlowDefinition
	if err := yaml.Unmarshal([]byte(yamlContent), &flowDef); err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}

	// Basic validation checks
	if flowDef.Metadata.Name == "" {
		return fmt.Errorf("flow name is required")
	}

	if len(flowDef.Nodes) == 0 {
		return fmt.Errorf("flow must have at least one node")
	}

	// Validate node types exist in factories or plugin registry
	for nodeName, nodeDef := range flowDef.Nodes {
		if _, exists := l.nodeFactories[nodeDef.Type]; !exists {
			if _, err := l.pluginRegistry.Get(nodeDef.Type); err != nil {
				return fmt.Errorf("unknown node type '%s' in node '%s'", nodeDef.Type, nodeName)
			}
		}
	}

	// Validate node references
	for nodeName, nodeDef := range flowDef.Nodes {
		for action, nextNode := range nodeDef.Next {
			if _, exists := flowDef.Nodes[nextNode]; !exists {
				return fmt.Errorf("node '%s' references non-existent node '%s' for action '%s'", nodeName, nextNode, action)
			}
		}
	}

	return nil
}

func findStartNode(flowDef FlowDefinition, nodes map[string]flowlib.Node) (flowlib.Node, error) {
	referencedNodes := make(map[string]bool)
	for _, nodeDef := range flowDef.Nodes {
		for _, nextNodeName := range nodeDef.Next {
			referencedNodes[nextNodeName] = true
		}
	}

	var startNodeName string
	for nodeName := range flowDef.Nodes {
		if !referencedNodes[nodeName] {
			if startNodeName != "" {
				return nil, fmt.Errorf("multiple start nodes found: '%s' and '%s'", startNodeName, nodeName)
			}
			startNodeName = nodeName
		}
	}

	if startNodeName == "" {
		return nil, fmt.Errorf("no start node found")
	}

	return nodes[startNodeName], nil
}
