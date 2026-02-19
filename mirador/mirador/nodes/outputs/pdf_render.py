"""PDF render output node — generates styled PDF table reports."""

from typing import Any
from pathlib import Path

from mirador.nodes.base import BaseNode, NodeMeta, NodePort


class PdfRenderNode(BaseNode):
    meta = NodeMeta(
        id="pdf_render",
        label="PDF Report",
        category="output",
        description="Render data as a styled PDF table report",
        inputs=[NodePort(name="in", description="Dataframe to render as PDF")],
        outputs=[],
        config_schema={
            "type": "object",
            "properties": {
                "output_path": {"type": "string", "title": "Output Path", "default": "report.pdf"},
                "page_size": {"type": "string", "title": "Page Size", "enum": ["A4", "Letter", "A3", "Legal"], "default": "A4"},
                "orientation": {"type": "string", "title": "Orientation", "enum": ["portrait", "landscape"], "default": "portrait"},
                "title": {"type": "string", "title": "Report Title", "default": ""},
                "subtitle": {"type": "string", "title": "Subtitle", "default": ""},
                "columns": {"type": "string", "title": "Columns (comma-sep, blank=all)", "default": ""},
                "max_rows": {"type": "integer", "title": "Max Rows", "default": 1000},
                "header_bg_color": {"type": "string", "title": "Header BG Color", "default": "#4b6777"},
                "header_text_color": {"type": "string", "title": "Header Text Color", "default": "#ffffff"},
                "font_family": {"type": "string", "title": "Font", "enum": ["Helvetica", "Times-Roman", "Courier"], "default": "Helvetica"},
                "font_size": {"type": "integer", "title": "Font Size", "default": 9},
                "show_header": {"type": "boolean", "title": "Show Page Header", "default": True},
                "show_footer": {"type": "boolean", "title": "Show Page Footer", "default": True},
                "footer_text": {"type": "string", "title": "Footer Text", "default": ""},
                "alternating_row_color": {"type": "string", "title": "Alt Row Color", "default": "#f0f4f7"},
            },
        },
    )

    def execute(self, inputs: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4, A3, letter, legal, landscape
            from reportlab.lib.styles import getSampleStyleSheet
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        except ImportError:
            return {"error": "reportlab not installed. Run: pip install reportlab"}

        table_data = inputs.get("df")
        if table_data is None:
            return {"error": "No input data"}

        all_columns = inputs.get("columns", [])
        if hasattr(table_data, "columns"):
            all_columns = table_data.columns

        # Determine columns to render
        cols_str = config.get("columns", "").strip()
        if cols_str:
            render_cols = [c.strip() for c in cols_str.split(",") if c.strip()]
        else:
            render_cols = list(all_columns)

        if not render_cols:
            return {"error": "No columns to render"}

        # Extract data
        n = len(table_data)
        max_rows = min(config.get("max_rows", 1000), n)
        data_dict = table_data.to_dict()

        # Build table rows
        header = render_cols
        rows = []
        for i in range(max_rows):
            row = [str(data_dict.get(col, {}).get(i, "")) if isinstance(data_dict.get(col), dict)
                   else str(data_dict[col][i]) if col in data_dict and i < len(data_dict[col])
                   else "" for col in render_cols]
            rows.append(row)

        # Page size
        page_sizes = {"A4": A4, "A3": A3, "Letter": letter, "Legal": legal}
        ps = page_sizes.get(config.get("page_size", "A4"), A4)
        if config.get("orientation", "portrait") == "landscape":
            ps = landscape(ps)

        # Output path
        output_path = config.get("output_path", "report.pdf")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        # Colors
        def hex_to_color(h: str):
            h = h.lstrip("#")
            if len(h) == 6:
                return colors.Color(int(h[0:2], 16)/255, int(h[2:4], 16)/255, int(h[4:6], 16)/255)
            return colors.gray

        header_bg = hex_to_color(config.get("header_bg_color", "#4b6777"))
        header_text = hex_to_color(config.get("header_text_color", "#ffffff"))
        alt_row = hex_to_color(config.get("alternating_row_color", "#f0f4f7"))
        font = config.get("font_family", "Helvetica")
        font_size = config.get("font_size", 9)

        title_text = config.get("title", "")
        subtitle_text = config.get("subtitle", "")
        show_header = config.get("show_header", True)
        show_footer = config.get("show_footer", True)
        footer_text = config.get("footer_text", "")

        # Build PDF
        def on_page(canvas, doc):
            if show_header and title_text:
                canvas.saveState()
                canvas.setFont(font, 10)
                canvas.drawString(doc.leftMargin, ps[1] - 30, title_text)
                canvas.restoreState()
            if show_footer:
                canvas.saveState()
                canvas.setFont(font, 8)
                ft = footer_text or f"Page {doc.page}"
                canvas.drawString(doc.leftMargin, 20, ft)
                canvas.restoreState()

        doc = SimpleDocTemplate(
            output_path,
            pagesize=ps,
            topMargin=50 if show_header and title_text else 30,
            bottomMargin=40 if show_footer else 20,
        )

        elements = []
        styles = getSampleStyleSheet()

        if title_text:
            elements.append(Paragraph(title_text, styles["Title"]))
        if subtitle_text:
            elements.append(Paragraph(subtitle_text, styles["Normal"]))
            elements.append(Spacer(1, 12))

        # Table
        table_content = [header] + rows
        t = Table(table_content, repeatRows=1)

        # Style
        style_commands = [
            ("BACKGROUND", (0, 0), (-1, 0), header_bg),
            ("TEXTCOLOR", (0, 0), (-1, 0), header_text),
            ("FONTNAME", (0, 0), (-1, 0), f"{font}-Bold" if font == "Helvetica" else font),
            ("FONTSIZE", (0, 0), (-1, 0), font_size + 1),
            ("FONTNAME", (0, 1), (-1, -1), font),
            ("FONTSIZE", (0, 1), (-1, -1), font_size),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.Color(0.8, 0.8, 0.8)),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]

        # Alternating row colors
        for i in range(1, len(table_content)):
            if i % 2 == 0:
                style_commands.append(("BACKGROUND", (0, i), (-1, i), alt_row))

        t.setStyle(TableStyle(style_commands))
        elements.append(t)

        doc.build(elements, onFirstPage=on_page, onLaterPages=on_page)

        import os
        file_size = os.path.getsize(output_path)

        return {
            "path": str(Path(output_path).resolve()),
            "size": file_size,
            "format": "pdf",
            "rows": max_rows,
            "columns": render_cols,
            "pages": doc.page,
        }
