"""
Simple Toga editor for the lookup CSV.
Table layout: first row = column names, then one row per record with editable cells.
Text fields for most columns; number input for numeric columns (e.g. interview_age).
Loads the file written by LookUpTable.write_lookup_table(); Save writes back the full CSV.
"""
import pathlib

import pandas
import toga


# Columns that should use NumberInput; everything else is TextInput
NUMERIC_COLUMNS = {"interview_age"}

# Minimum column width in pixels
MIN_COLUMN_WIDTH = 50
# Subject/session ID columns often need more space; enforce a higher minimum
MIN_WIDTH_SUBJECT_COLUMNS = 220
SUBJECT_LIKE_COLUMNS = {"bids_subject_session", "src_subject_id", "subjectkey"}
# Pixels per character estimate for calculating column width
PIXELS_PER_CHAR = 10


def run_lookup_editor(csv_path: pathlib.Path) -> None:
    """Run the Toga app that opens and edits the given lookup CSV. Blocks until the window is closed."""
    app = LookupEditorApp(str(csv_path))
    app.main_loop()


class LookupEditorApp(toga.App):
    def __init__(self, csv_path: str, **kwargs):
        self.csv_path = pathlib.Path(csv_path)
        super().__init__("Lookup CSV Editor", "org.ndabids.lookup_editor", **kwargs)

    def startup(self):
        self.main_window = toga.MainWindow(title=self.formal_name, size=(900, 500))

        # Resolve ~ and any relative path so we read/write the actual file
        self.csv_path = self.csv_path.expanduser().resolve()

        try:
            df = pandas.read_csv(self.csv_path)
        except Exception as e:
            self.main_window.content = toga.Box(
                children=[toga.Label(f"Cannot load CSV: {e}", margin=10)],
                direction="column",
                margin=10,
            )
            self.main_window.show()
            return

        columns = list(df.columns)
        df = df.fillna("")

        # Calculate minimum width for each column based on longest entry
        # Include column name in the max calculation
        column_widths = {}
        for col in columns:
            max_length = len(col)
            for _, row in df.iterrows():
                val = row[col]
                if not pandas.isna(val) and val != "":
                    val_str = str(val).strip()
                    max_length = max(max_length, len(val_str))
            # Convert to pixels with minimum; subject-like columns get a higher floor
            min_w = MIN_WIDTH_SUBJECT_COLUMNS if col in SUBJECT_LIKE_COLUMNS else MIN_COLUMN_WIDTH
            column_widths[col] = max(min_w, max_length * PIXELS_PER_CHAR)

        # Build table as a ROW of COLUMNS (not row-by-row). Each column is one fixed-width Box
        # so Cocoa applies width to every column, not just the first.
        self.columns = columns
        num_rows = len(df)
        self.cell_rows = [{} for _ in range(num_rows)]
        self._initial_values = [{} for _ in range(num_rows)]
        column_boxes = []

        for col in columns:
            # Header for this column
            header_label = toga.Label(col, margin=3, flex=1)
            col_cell_widgets = []
            # One cell per data row
            for row_idx, (_, row) in enumerate(df.iterrows()):
                raw = row[col]
                if pandas.isna(raw) or raw == "" or raw is None:
                    cell_val = ""
                else:
                    cell_val = str(raw).strip()

                if col in NUMERIC_COLUMNS:
                    try:
                        default_num = int(float(cell_val)) if cell_val else None
                    except (TypeError, ValueError):
                        default_num = None
                    w = toga.NumberInput(value=default_num, margin=3, flex=1)
                    self.cell_rows[row_idx][col] = w
                    self._initial_values[row_idx][col] = default_num
                else:
                    default_text = cell_val if isinstance(cell_val, str) else str(cell_val)
                    w = toga.TextInput(value="", margin=3, flex=1)
                    self.cell_rows[row_idx][col] = w
                    self._initial_values[row_idx][col] = default_text
                col_cell_widgets.append(w)
            # One column = one fixed-width Box with header + all cells
            column_box = toga.Box(
                children=[header_label] + col_cell_widgets,
                direction="column",
                width=column_widths[col],
                margin=3,
            )
            column_boxes.append(column_box)

        # Force row to explicit total width so Pack allocates each column its width (Cocoa fix).
        total_table_width = sum(column_widths[col] for col in columns) + (len(columns) * 6)  # 3px margin each side per col
        table_content = toga.Box(
            children=column_boxes,
            direction="row",
            margin=3,
            width=total_table_width,
        )

        async def save_handler(widget, **kwargs):
            rows = []
            for row_widgets_by_col in self.cell_rows:
                out_row = {}
                for col in self.columns:
                    w = row_widgets_by_col[col]
                    if isinstance(w, toga.NumberInput):
                        v = w.value
                        out_row[col] = int(v) if v is not None and not pandas.isna(v) else ""
                    else:
                        out_row[col] = (w.value or "")
                rows.append(out_row)
            out = pandas.DataFrame(rows, columns=self.columns)
            out.to_csv(self.csv_path, sep=",", na_rep="n/a", index=False)
            await self.main_window.dialog(toga.InfoDialog("Saved", f"Saved to {self.csv_path}"))

        save_btn = toga.Button("Save", on_press=save_handler, margin=5)

        top_bar = toga.Box(
            children=[
                toga.Label(f"Editing: {self.csv_path}", margin=5),
                save_btn,
            ],
            direction="row",
            margin=5,
        )

        content = toga.Box(
            children=[top_bar, table_content],
            direction="column",
            margin=10,
        )
        scroll = toga.ScrollContainer(content=content)
        self.main_window.content = scroll
        self.main_window.show()
        # Set initial values after window is shown so native controls have correct content
        self._apply_initial_values()

    def _apply_initial_values(self) -> None:
        """Set each cell widget's value from the loaded CSV. Call after window is shown."""
        for row_widgets_by_col, initial_vals in zip(self.cell_rows, self._initial_values):
            for col in self.columns:
                w = row_widgets_by_col[col]
                val = initial_vals.get(col, "")
                if isinstance(w, toga.NumberInput):
                    w.value = val if val is not None else None
                else:
                    w.value = str(val) if val != "" else ""
