class GraphBuilder:
    """
    Build a dependency graph for normalized IDMC mappings.

    Supports:
      - New XML ID links: SRC1 -> EXP1
      - Legacy links: EXP1.OUT -> FLT1.IN
      - Router branch links: RTR1:HIGH_VALUE -> TGT1

    Note:
      A lookup or other transformation with an internal reference
      may require additional dependency edges if the XML defines
      those dependencies separately.
    """

    def build(self, mapping):
        sources = mapping.get("source_instances", [])
        transformations = mapping.get("transformations", [])
        targets = mapping.get("target_instances", [])
        links = mapping.get("links", [])

        nodes = []
        id_to_name = {}
        name_to_node = {}

        def add_node(node_id, name, node_type, metadata=None):
            if not name:
                raise ValueError(
                    f"Graph node has no name: id={node_id!r}"
                )

            node = {
                "id": node_id,
                "name": name,
                "type": node_type,
                "metadata": metadata or {}
            }

            if name in name_to_node:
                raise ValueError(
                    f"Duplicate graph node name: {name}"
                )

            nodes.append(node)
            name_to_node[name] = node

            if node_id:
                if node_id in id_to_name:
                    raise ValueError(
                        f"Duplicate graph node ID: {node_id}"
                    )
                id_to_name[node_id] = name

        for source in sources:
            add_node(
                source.get("id"),
                source.get("name"),
                "source",
                source
            )

        for transformation in transformations:
            add_node(
                transformation.get("id"),
                transformation.get("name"),
                "transformation",
                transformation
            )

        for target in targets:
            add_node(
                target.get("id"),
                target.get("name"),
                "target",
                target
            )

        def resolve_endpoint(value):
            """
            Resolve an endpoint to a graph node.

            New XML:
                SRC1
                RTR1:HIGH_VALUE

            Legacy XML:
                EXP1.OUT
            """
            if not value:
                raise ValueError("Empty graph link endpoint")

            value = str(value).strip()

            # Router branch syntax: NODE:BRANCH
            branch_label = None
            if ":" in value:
                possible_id, possible_branch = value.split(":", 1)
                if possible_id in id_to_name:
                    value = possible_id
                    branch_label = possible_branch

            # Legacy port syntax: NODE.PORT
            port_name = None
            if "." in value:
                possible_id, possible_port = value.split(".", 1)

                if possible_id in id_to_name:
                    value = possible_id
                    port_name = possible_port
                elif value in name_to_node:
                    # A literal node name containing a dot.
                    pass

            if value in id_to_name:
                node_name = id_to_name[value]
            elif value in name_to_node:
                node_name = value
            else:
                raise ValueError(
                    f"Link references unknown node: {value!r}"
                )

            return node_name, port_name, branch_label

        edges = []

        for link in links:
            from_value = link.get("from")
            to_value = link.get("to")

            if not from_value or not to_value:
                raise ValueError(
                    f"Invalid link; missing from/to: {link}"
                )

            source_name, source_port, source_branch = (
                resolve_endpoint(from_value)
            )
            target_name, target_port, target_branch = (
                resolve_endpoint(to_value)
            )

            edges.append({
                "from": source_name,
                "from_id": (
                    from_value.split(":", 1)[0].split(".", 1)[0]
                ),
                "from_port": source_port,
                "from_branch": source_branch,
                "to": target_name,
                "to_id": (
                    to_value.split(":", 1)[0].split(".", 1)[0]
                ),
                "to_port": target_port,
                "to_branch": target_branch,
                "attributes": dict(link.get("attributes", {}))
            })

        node_names = [node["name"] for node in nodes]
        execution_order = self._topological_sort(
            node_names,
            edges
        )

        return {
            "nodes": nodes,
            "edges": edges,
            "execution_order": execution_order
        }

    def _topological_sort(self, nodes, edges):
        dependencies = {node: set() for node in nodes}

        for edge in edges:
            source = edge["from"]
            target = edge["to"]

            if source == target:
                raise ValueError(
                    f"Self-referencing graph edge: {source}"
                )

            dependencies[target].add(source)

        order = []
        remaining = {
            node: set(deps)
            for node, deps in dependencies.items()
        }

        while remaining:
            ready = sorted(
                node
                for node, deps in remaining.items()
                if not deps
            )

            if not ready:
                cyclic_nodes = sorted(remaining)
                raise ValueError(
                    "Cycle detected in mapping graph. "
                    f"Unresolved nodes: {cyclic_nodes}"
                )

            for node in ready:
                order.append(node)
                del remaining[node]

            for deps in remaining.values():
                deps.difference_update(ready)

        return order
