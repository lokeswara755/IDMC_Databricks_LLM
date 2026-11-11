"""Build a dependency graph from normalized IDMC/PowerCenter mappings.

The builder only creates nodes and edges from explicit mapping definitions.
A session's mapping-name reference is not a mapping definition and must not
be passed here as if it contained source/target/transformation lineage.
"""


class GraphBuilder:
    """Build and validate a directed acyclic graph for one mapping."""

    def build(self, mapping):
        if mapping is None:
            raise ValueError("Mapping input cannot be None")
        if not isinstance(mapping, dict):
            raise TypeError("Mapping input must be a dictionary")

        sources = mapping.get("source_instances") or []
        transformations = mapping.get("transformations") or []
        targets = mapping.get("target_instances") or []
        links = mapping.get("links") or []

        # No definitions means no graph. Return an explicit state instead of
        # manufacturing nodes from SESSION/@MAPPINGNAME references.
        if not any((sources, transformations, targets, links)):
            return {
                "mapping_name": mapping.get("name") or mapping.get("mapping_name"),
                "status": "NO_MAPPING_DEFINITIONS",
                "nodes": [],
                "edges": [],
                "execution_order": [],
                "diagnostics": [
                    "No source, transformation, target or connector definitions were supplied."
                ],
            }

        nodes = []
        id_to_name = {}
        name_to_node = {}

        def add_node(item, node_type):
            if not isinstance(item, dict):
                raise TypeError(f"{node_type} entry must be a dictionary: {item!r}")
            node_id = self._first(item, "id", "INSTANCEID", "instance_id")
            name = self._first(item, "name", "NAME", "instance_name")
            if not name:
                # PowerCenter instance records may use transformation_name;
                # accept it only when explicitly supplied.
                name = self._first(item, "transformation_name", "object_name")
            if not name:
                raise ValueError(f"Graph {node_type} has no name: id={node_id!r}")
            name = str(name).strip()
            node_id = str(node_id).strip() if node_id is not None else None
            if name in name_to_node:
                raise ValueError(f"Duplicate graph node name: {name}")
            if node_id and node_id in id_to_name:
                raise ValueError(f"Duplicate graph node ID: {node_id}")
            node = {"id": node_id, "name": name, "type": node_type, "metadata": item}
            nodes.append(node)
            name_to_node[name] = node
            if node_id:
                id_to_name[node_id] = name

        for item in sources:
            add_node(item, "source")
        for item in transformations:
            add_node(item, "transformation")
        for item in targets:
            add_node(item, "target")

        def resolve_endpoint(raw_value, explicit_port=None, explicit_branch=None):
            if raw_value is None or not str(raw_value).strip():
                raise ValueError("Empty graph link endpoint")
            value = str(raw_value).strip()
            port_name = explicit_port
            branch_label = explicit_branch

            # Router branch notation: ROUTER:GROUP. Only split when the
            # prefix is a known node ID or name.
            if ":" in value:
                prefix, suffix = value.split(":", 1)
                if prefix in id_to_name or prefix in name_to_node:
                    value, branch_label = prefix, branch_label or suffix

            # Port notation: INSTANCE.FIELD. Preserve literal dotted names.
            if "." in value:
                prefix, suffix = value.split(".", 1)
                if prefix in id_to_name or prefix in name_to_node:
                    value, port_name = prefix, port_name or suffix

            if value in id_to_name:
                node_name = id_to_name[value]
            elif value in name_to_node:
                node_name = value
            else:
                raise ValueError(f"Link references unknown node: {value!r}")
            return node_name, port_name, branch_label

        edges = []
        for index, link in enumerate(links):
            if not isinstance(link, dict):
                raise TypeError(f"Link #{index + 1} must be a dictionary")

            from_value = self._first(link, "from", "FROM", "from_instance", "FROMINSTANCE")
            to_value = self._first(link, "to", "TO", "to_instance", "TOINSTANCE")
            from_port = self._first(link, "from_port", "FROMFIELD", "from_field")
            to_port = self._first(link, "to_port", "TOFIELD", "to_field")
            from_branch = self._first(link, "from_branch", "branch", "GROUP")
            to_branch = self._first(link, "to_branch")

            if from_value is None or to_value is None:
                raise ValueError(
                    f"Invalid link #{index + 1}: expected from/to endpoints; got {link!r}"
                )

            source_name, source_port, source_branch = resolve_endpoint(
                from_value, from_port, from_branch
            )
            target_name, target_port, target_branch = resolve_endpoint(
                to_value, to_port, to_branch
            )
            source_id = self._node_id(source_name, name_to_node)
            target_id = self._node_id(target_name, name_to_node)

            edges.append({
                "from": source_name,
                "from_id": source_id,
                "from_port": source_port,
                "from_branch": source_branch,
                "to": target_name,
                "to_id": target_id,
                "to_port": target_port,
                "to_branch": target_branch,
                "attributes": dict(link.get("attributes") or {}),
                "metadata": link,
            })

        node_names = [node["name"] for node in nodes]
        execution_order = self._topological_sort(node_names, edges)
        return {
            "mapping_name": mapping.get("name") or mapping.get("mapping_name"),
            "status": "GRAPH_BUILT",
            "nodes": nodes,
            "edges": edges,
            "execution_order": execution_order,
            "diagnostics": [],
        }

    @staticmethod
    def _first(data, *keys):
        for key in keys:
            value = data.get(key)
            if value is not None and str(value).strip() != "":
                return value
        return None

    @staticmethod
    def _node_id(name, name_to_node):
        return name_to_node[name].get("id")

    def _topological_sort(self, nodes, edges):
        dependencies = {node: set() for node in nodes}
        for edge in edges:
            source, target = edge["from"], edge["to"]
            if source not in dependencies or target not in dependencies:
                raise ValueError(f"Edge references node outside graph: {source!r} -> {target!r}")
            if source == target:
                raise ValueError(f"Self-referencing graph edge: {source}")
            dependencies[target].add(source)

        order = []
        remaining = {node: set(deps) for node, deps in dependencies.items()}
        while remaining:
            ready = sorted(node for node, deps in remaining.items() if not deps)
            if not ready:
                raise ValueError(
                    "Cycle detected in mapping graph. Unresolved nodes: "
                    f"{sorted(remaining)}"
                )
            for node in ready:
                order.append(node)
                del remaining[node]
            for deps in remaining.values():
                deps.difference_update(ready)
        return order
