"""
RoadReport - Reporting & Export System for BlackRoad
Generate reports, export data, and schedule report delivery.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import asyncio
import csv
import hashlib
import io
import json
import logging
import threading
import uuid

logger = logging.getLogger(__name__)


class ReportFormat(str, Enum):
    """Report output formats."""
    JSON = "json"
    CSV = "csv"
    HTML = "html"
    PDF = "pdf"
    EXCEL = "excel"
    MARKDOWN = "markdown"


class ReportStatus(str, Enum):
    """Report generation status."""
    PENDING = "pending"
    GENERATING = "generating"
    COMPLETED = "completed"
    FAILED = "failed"


class ScheduleFrequency(str, Enum):
    """Report schedule frequency."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


@dataclass
class ReportColumn:
    """A report column definition."""
    name: str
    field: str
    formatter: Optional[Callable[[Any], str]] = None
    width: Optional[int] = None
    sortable: bool = True
    filterable: bool = True


@dataclass
class ReportFilter:
    """A report filter."""
    field: str
    operator: str  # eq, ne, gt, lt, gte, lte, contains, in
    value: Any


@dataclass
class ReportDefinition:
    """Definition of a report."""
    id: str
    name: str
    description: str = ""
    columns: List[ReportColumn] = field(default_factory=list)
    data_source: Optional[Callable[[], List[Dict]]] = None
    filters: List[ReportFilter] = field(default_factory=list)
    sort_by: Optional[str] = None
    sort_order: str = "asc"
    group_by: Optional[str] = None
    aggregations: Dict[str, str] = field(default_factory=dict)  # field -> agg function
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GeneratedReport:
    """A generated report."""
    id: str
    definition_id: str
    format: ReportFormat
    status: ReportStatus = ReportStatus.PENDING
    content: Optional[bytes] = None
    row_count: int = 0
    generated_at: Optional[datetime] = None
    generation_time_ms: float = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReportSchedule:
    """A report schedule."""
    id: str
    definition_id: str
    format: ReportFormat
    frequency: ScheduleFrequency
    recipients: List[str] = field(default_factory=list)
    next_run: Optional[datetime] = None
    last_run: Optional[datetime] = None
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataProcessor:
    """Process report data."""

    def filter(self, data: List[Dict], filters: List[ReportFilter]) -> List[Dict]:
        """Apply filters to data."""
        result = data
        
        for f in filters:
            result = [
                row for row in result
                if self._apply_filter(row.get(f.field), f.operator, f.value)
            ]
        
        return result

    def _apply_filter(self, value: Any, operator: str, filter_value: Any) -> bool:
        if operator == "eq":
            return value == filter_value
        elif operator == "ne":
            return value != filter_value
        elif operator == "gt":
            return value > filter_value
        elif operator == "lt":
            return value < filter_value
        elif operator == "gte":
            return value >= filter_value
        elif operator == "lte":
            return value <= filter_value
        elif operator == "contains":
            return filter_value in str(value)
        elif operator == "in":
            return value in filter_value
        return True

    def sort(self, data: List[Dict], field: str, order: str = "asc") -> List[Dict]:
        """Sort data."""
        reverse = order.lower() == "desc"
        return sorted(data, key=lambda x: x.get(field, ""), reverse=reverse)

    def group(self, data: List[Dict], field: str) -> Dict[str, List[Dict]]:
        """Group data by field."""
        groups: Dict[str, List[Dict]] = {}
        for row in data:
            key = str(row.get(field, ""))
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        return groups

    def aggregate(
        self,
        data: List[Dict],
        aggregations: Dict[str, str]
    ) -> Dict[str, Any]:
        """Apply aggregations."""
        result = {}
        
        for field, agg in aggregations.items():
            values = [row.get(field) for row in data if row.get(field) is not None]
            
            if agg == "sum":
                result[field] = sum(values)
            elif agg == "avg":
                result[field] = sum(values) / len(values) if values else 0
            elif agg == "min":
                result[field] = min(values) if values else None
            elif agg == "max":
                result[field] = max(values) if values else None
            elif agg == "count":
                result[field] = len(values)
        
        return result


class ReportFormatter:
    """Format reports for output."""

    def format(
        self,
        definition: ReportDefinition,
        data: List[Dict],
        output_format: ReportFormat
    ) -> bytes:
        """Format data to specified format."""
        if output_format == ReportFormat.JSON:
            return self._to_json(definition, data)
        elif output_format == ReportFormat.CSV:
            return self._to_csv(definition, data)
        elif output_format == ReportFormat.HTML:
            return self._to_html(definition, data)
        elif output_format == ReportFormat.MARKDOWN:
            return self._to_markdown(definition, data)
        else:
            return self._to_json(definition, data)

    def _to_json(self, definition: ReportDefinition, data: List[Dict]) -> bytes:
        output = {
            "report": definition.name,
            "generated_at": datetime.now().isoformat(),
            "row_count": len(data),
            "columns": [c.name for c in definition.columns],
            "data": data
        }
        return json.dumps(output, indent=2, default=str).encode()

    def _to_csv(self, definition: ReportDefinition, data: List[Dict]) -> bytes:
        output = io.StringIO()
        fields = [c.field for c in definition.columns]
        headers = [c.name for c in definition.columns]
        
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
        writer.writerow(dict(zip(fields, headers)))
        
        for row in data:
            formatted_row = {}
            for col in definition.columns:
                value = row.get(col.field, "")
                if col.formatter:
                    value = col.formatter(value)
                formatted_row[col.field] = value
            writer.writerow(formatted_row)
        
        return output.getvalue().encode()

    def _to_html(self, definition: ReportDefinition, data: List[Dict]) -> bytes:
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{definition.name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>{definition.name}</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p>Rows: {len(data)}</p>
    <table>
        <tr>
"""
        for col in definition.columns:
            html += f"            <th>{col.name}</th>\n"
        html += "        </tr>\n"
        
        for row in data:
            html += "        <tr>\n"
            for col in definition.columns:
                value = row.get(col.field, "")
                if col.formatter:
                    value = col.formatter(value)
                html += f"            <td>{value}</td>\n"
            html += "        </tr>\n"
        
        html += """    </table>
</body>
</html>"""
        return html.encode()

    def _to_markdown(self, definition: ReportDefinition, data: List[Dict]) -> bytes:
        lines = [f"# {definition.name}", "", f"Generated: {datetime.now().isoformat()}", ""]
        
        # Header
        headers = [col.name for col in definition.columns]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
        
        # Rows
        for row in data:
            values = []
            for col in definition.columns:
                value = row.get(col.field, "")
                if col.formatter:
                    value = col.formatter(value)
                values.append(str(value))
            lines.append("| " + " | ".join(values) + " |")
        
        return "\n".join(lines).encode()


class ReportStore:
    """Store for reports."""

    def __init__(self):
        self.definitions: Dict[str, ReportDefinition] = {}
        self.reports: Dict[str, GeneratedReport] = {}
        self.schedules: Dict[str, ReportSchedule] = {}
        self._lock = threading.Lock()

    def save_definition(self, definition: ReportDefinition) -> None:
        with self._lock:
            self.definitions[definition.id] = definition

    def get_definition(self, def_id: str) -> Optional[ReportDefinition]:
        return self.definitions.get(def_id)

    def save_report(self, report: GeneratedReport) -> None:
        with self._lock:
            self.reports[report.id] = report

    def get_report(self, report_id: str) -> Optional[GeneratedReport]:
        return self.reports.get(report_id)

    def save_schedule(self, schedule: ReportSchedule) -> None:
        with self._lock:
            self.schedules[schedule.id] = schedule


class ReportGenerator:
    """Generate reports."""

    def __init__(self, store: ReportStore):
        self.store = store
        self.processor = DataProcessor()
        self.formatter = ReportFormatter()

    async def generate(
        self,
        definition: ReportDefinition,
        output_format: ReportFormat,
        filters: List[ReportFilter] = None
    ) -> GeneratedReport:
        """Generate a report."""
        import time
        start_time = time.time()

        report = GeneratedReport(
            id=str(uuid.uuid4()),
            definition_id=definition.id,
            format=output_format,
            status=ReportStatus.GENERATING
        )

        try:
            # Get data
            if definition.data_source:
                data = definition.data_source()
            else:
                data = []

            # Apply filters
            all_filters = definition.filters + (filters or [])
            data = self.processor.filter(data, all_filters)

            # Sort
            if definition.sort_by:
                data = self.processor.sort(data, definition.sort_by, definition.sort_order)

            # Format output
            report.content = self.formatter.format(definition, data, output_format)
            report.row_count = len(data)
            report.status = ReportStatus.COMPLETED
            report.generated_at = datetime.now()

        except Exception as e:
            report.status = ReportStatus.FAILED
            report.error = str(e)
            logger.error(f"Report generation failed: {e}")

        report.generation_time_ms = (time.time() - start_time) * 1000
        self.store.save_report(report)

        return report


class ReportManager:
    """High-level report management."""

    def __init__(self):
        self.store = ReportStore()
        self.generator = ReportGenerator(self.store)

    def define_report(
        self,
        name: str,
        columns: List[Dict[str, Any]],
        data_source: Callable[[], List[Dict]],
        description: str = "",
        **kwargs
    ) -> ReportDefinition:
        """Define a new report."""
        definition = ReportDefinition(
            id=hashlib.md5(f"{name}{datetime.now()}".encode()).hexdigest()[:12],
            name=name,
            description=description,
            columns=[
                ReportColumn(
                    name=c.get("name", c.get("field")),
                    field=c.get("field"),
                    formatter=c.get("formatter"),
                    width=c.get("width")
                )
                for c in columns
            ],
            data_source=data_source,
            **kwargs
        )
        
        self.store.save_definition(definition)
        return definition

    async def generate(
        self,
        definition_id: str,
        output_format: ReportFormat = ReportFormat.JSON,
        filters: List[Dict[str, Any]] = None
    ) -> Optional[GeneratedReport]:
        """Generate a report."""
        definition = self.store.get_definition(definition_id)
        if not definition:
            return None

        filter_objects = [
            ReportFilter(field=f["field"], operator=f["operator"], value=f["value"])
            for f in (filters or [])
        ]

        return await self.generator.generate(definition, output_format, filter_objects)

    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Get generated report."""
        report = self.store.get_report(report_id)
        if report:
            return {
                "id": report.id,
                "status": report.status.value,
                "format": report.format.value,
                "row_count": report.row_count,
                "generated_at": report.generated_at.isoformat() if report.generated_at else None,
                "generation_time_ms": report.generation_time_ms
            }
        return None

    def download_report(self, report_id: str) -> Optional[bytes]:
        """Download report content."""
        report = self.store.get_report(report_id)
        return report.content if report else None

    def schedule_report(
        self,
        definition_id: str,
        frequency: ScheduleFrequency,
        output_format: ReportFormat = ReportFormat.CSV,
        recipients: List[str] = None
    ) -> ReportSchedule:
        """Schedule a report."""
        schedule = ReportSchedule(
            id=str(uuid.uuid4()),
            definition_id=definition_id,
            format=output_format,
            frequency=frequency,
            recipients=recipients or [],
            next_run=self._calculate_next_run(frequency)
        )
        
        self.store.save_schedule(schedule)
        return schedule

    def _calculate_next_run(self, frequency: ScheduleFrequency) -> datetime:
        now = datetime.now()
        
        if frequency == ScheduleFrequency.DAILY:
            return now + timedelta(days=1)
        elif frequency == ScheduleFrequency.WEEKLY:
            return now + timedelta(weeks=1)
        elif frequency == ScheduleFrequency.MONTHLY:
            return now + timedelta(days=30)
        elif frequency == ScheduleFrequency.QUARTERLY:
            return now + timedelta(days=90)
        
        return now + timedelta(days=1)


# Example usage
async def example_usage():
    """Example report usage."""
    manager = ReportManager()

    # Sample data source
    def get_sales_data():
        return [
            {"product": "Widget A", "quantity": 100, "revenue": 5000, "region": "North"},
            {"product": "Widget B", "quantity": 75, "revenue": 3750, "region": "South"},
            {"product": "Widget C", "quantity": 200, "revenue": 8000, "region": "North"},
            {"product": "Widget A", "quantity": 50, "revenue": 2500, "region": "East"},
        ]

    # Define report
    definition = manager.define_report(
        name="Sales Report",
        description="Monthly sales summary",
        columns=[
            {"field": "product", "name": "Product"},
            {"field": "quantity", "name": "Quantity"},
            {"field": "revenue", "name": "Revenue", "formatter": lambda x: f"${x:,.2f}"},
            {"field": "region", "name": "Region"}
        ],
        data_source=get_sales_data,
        sort_by="revenue",
        sort_order="desc"
    )

    # Generate reports
    json_report = await manager.generate(definition.id, ReportFormat.JSON)
    csv_report = await manager.generate(definition.id, ReportFormat.CSV)
    html_report = await manager.generate(definition.id, ReportFormat.HTML)

    print(f"JSON Report: {manager.get_report(json_report.id)}")
    print(f"\nCSV Content:\n{csv_report.content.decode()}")
