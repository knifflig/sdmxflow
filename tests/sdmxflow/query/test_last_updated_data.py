from __future__ import annotations

import datetime as dt

from sdmxflow.query.last_updated_data import extract_last_updated_data_from_dataflow_xml


def test_extract_last_updated_prefers_update_data() -> None:
    xml = b"""<?xml version='1.0' encoding='utf-8'?>
<m:Structure xmlns:m='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
             xmlns:c='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
             xmlns:s='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'>
  <s:Dataflows>
    <s:Dataflow id='X'>
      <c:Annotations>
        <c:Annotation>
          <c:AnnotationTitle>2026-01-09T23:00:00+0100</c:AnnotationTitle>
          <c:AnnotationType>DISSEMINATION_TIMESTAMP_DATA</c:AnnotationType>
        </c:Annotation>
        <c:Annotation>
          <c:AnnotationTitle>2026-01-10T01:00:00+0100</c:AnnotationTitle>
          <c:AnnotationType>UPDATE_DATA</c:AnnotationType>
        </c:Annotation>
      </c:Annotations>
    </s:Dataflow>
  </s:Dataflows>
</m:Structure>
"""
    got = extract_last_updated_data_from_dataflow_xml(xml)
    assert got == dt.datetime(2026, 1, 10, 0, 0, 0, tzinfo=dt.UTC)


def test_extract_last_updated_falls_back_to_dissemination_ts() -> None:
    xml = b"""<?xml version='1.0' encoding='utf-8'?>
<m:Structure xmlns:m='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message'
             xmlns:c='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
             xmlns:s='http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'>
  <s:Dataflows>
    <s:Dataflow id='X'>
      <c:Annotations>
        <c:Annotation>
          <c:AnnotationTitle>2026-01-09T23:00:00+0100</c:AnnotationTitle>
          <c:AnnotationType>DISSEMINATION_TIMESTAMP_DATA</c:AnnotationType>
        </c:Annotation>
      </c:Annotations>
    </s:Dataflow>
  </s:Dataflows>
</m:Structure>
"""
    got = extract_last_updated_data_from_dataflow_xml(xml)
    assert got == dt.datetime(2026, 1, 9, 22, 0, 0, tzinfo=dt.UTC)
