class MetadataRelationshipParser:

    def parse(self, root):
        container = next(
            (
                element for element in root.iter()
                if element.tag == "MetadataRelationships"
            ),
            None,
        )

        if container is None:
            raise ValueError("MetadataRelationships not found")

        relationships = [
            {
                "id": element.get("id"),
                "from": element.get("from"),
                "type": element.get("type"),
                "to": element.get("to"),
                "attributes": dict(element.attrib),
            }
            for element in container
            if element.tag == "Relationship"
        ]

        return {
            "relationships": relationships,
            "summary": {
                "relationship_count": len(relationships),
                "missing_id_count": sum(
                    not r["id"] for r in relationships
                ),
                "missing_from_count": sum(
                    not r["from"] for r in relationships
                ),
                "missing_type_count": sum(
                    not r["type"] for r in relationships
                ),
                "missing_to_count": sum(
                    not r["to"] for r in relationships
                ),
            },
        }