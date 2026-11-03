class CrossReferenceValidator:

    ASSET_TAGS = {
        "Mapping",
        "MappingTask",
        "Taskflow",
        "ReusableTaskflow",
        "FileRuntimeParameters",
        "DataQualityRule",
        "DataIngestionJob",
        "API",
        "ApplicationIntegrationProcess",
        "Connection",
        "Folder",
    }

    def validate(self, root):
        asset_index = {}
        duplicate_ids = []

        # Build an index of actual asset IDs from the XML.
        for element in root.iter():
            if element.tag not in self.ASSET_TAGS:
                continue

            asset_id = element.get("id")
            if not asset_id:
                continue

            asset_index.setdefault(asset_id, []).append(element.tag)

        duplicate_ids = {
            asset_id: tags
            for asset_id, tags in asset_index.items()
            if len(tags) > 1
        }

        # Validate metadata relationship endpoints.
        relationships_container = next(
            (
                element for element in root.iter()
                if element.tag == "MetadataRelationships"
            ),
            None,
        )

        if relationships_container is None:
            raise ValueError("MetadataRelationships not found")

        relationship_errors = []

        for relationship in relationships_container:
            if relationship.tag != "Relationship":
                continue

            for field in ("from", "to"):
                reference = relationship.get(field)

                if not reference:
                    relationship_errors.append({
                        "relationship_id": relationship.get("id"),
                        "field": field,
                        "reference": reference,
                        "error": "Missing reference",
                    })
                elif reference not in asset_index:
                    relationship_errors.append({
                        "relationship_id": relationship.get("id"),
                        "field": field,
                        "reference": reference,
                        "error": "Unresolved asset reference",
                    })

        # Validate AI recommendation targets.
        recommendation_errors = []

        for element in root.iter():
            if element.tag != "Recommendation":
                continue

            target_id = element.get("targetObjectId")

            if not target_id or target_id not in asset_index:
                recommendation_errors.append({
                    "recommendation_id": element.get("id"),
                    "target": target_id,
                    "error": (
                        "Missing target"
                        if not target_id
                        else "Unresolved asset reference"
                    ),
                })

        # Validate API -> ApplicationIntegrationProcess references.
        process_ids = {
            element.get("id")
            for element in root.iter()
            if element.tag == "ApplicationIntegrationProcess"
            and element.get("id")
        }

        api_errors = []

        for api in root.iter():
            if api.tag != "API":
                continue

            for process_ref in api.findall("ProcessRef"):
                process_id = process_ref.get("objectId")

                if not process_id or process_id not in process_ids:
                    api_errors.append({
                        "api_id": api.get("id"),
                        "process_id": process_id,
                        "error": (
                            "Missing process reference"
                            if not process_id
                            else "Unresolved process reference"
                        ),
                    })

        return {
            "summary": {
                "asset_id_count": len(asset_index),
                "duplicate_asset_id_count": len(duplicate_ids),
                "relationship_count": len(
                    relationships_container.findall("Relationship")
                ),
                "relationship_error_count": len(relationship_errors),
                "recommendation_count": sum(
                    element.tag == "Recommendation"
                    for element in root.iter()
                ),
                "recommendation_error_count": len(recommendation_errors),
                "api_process_reference_error_count": len(api_errors),
            },
            "duplicate_asset_ids": duplicate_ids,
            "relationship_errors": relationship_errors,
            "recommendation_errors": recommendation_errors,
            "api_process_errors": api_errors,
        }