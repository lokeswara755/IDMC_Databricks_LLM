
class DataIngestionJobParser:

    def parse(self, root):
        assets = root.find("./Project/Assets")

        if assets is None:
            raise ValueError("Project/Assets not found")

        jobs = [
            self._parse_job(element)
            for element in assets
            if element.tag == "DataIngestionJob"
        ]

        return {
            "ingestion_jobs": jobs,
            "summary": {
                "job_count": len(jobs),
                "source_count": sum(
                    len(job["sources"]) for job in jobs
                ),
                "landing_target_count": sum(
                    len(job["landing_targets"]) for job in jobs
                ),
                "downstream_mapping_ref_count": sum(
                    len(job["downstream_mapping_refs"])
                    for job in jobs
                )
            }
        }

    def _parse_job(self, element):
        sources = []
        landing_targets = []
        mapping_refs = []
        extensions = []

        for child in element:
            if child.tag == "Source":
                sources.append(self._parse_element(child))

            elif child.tag == "LandingTarget":
                landing_targets.append(
                    self._parse_element(child)
                )

            elif child.tag == "DownstreamMappingRef":
                mapping_refs.append({
                    "object_id": child.get("objectId"),
                    "attributes": dict(child.attrib),
                    "raw_xml": self._parse_element(child)
                })

            else:
                extensions.append(self._parse_element(child))

        return {
            "id": element.get("id"),
            "name": element.get("name"),
            "status": element.get("status"),
            "ingestion_mode": element.get("ingestionMode"),
            "schedule": element.get("schedule"),
            "attributes": dict(element.attrib),
            "sources": sources,
            "landing_targets": landing_targets,
            "downstream_mapping_refs": mapping_refs,
            "extensions": extensions,
            "raw_xml": self._parse_element(element)
        }

    @staticmethod
    def _parse_element(element):
        return {
            "tag": element.tag,
            "attributes": dict(element.attrib),
            "text": (element.text or "").strip() or None,
            "children": [
                DataIngestionJobParser._parse_element(child)
                for child in element
            ]
        }
