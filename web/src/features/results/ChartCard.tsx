import { useId, useState, type ReactNode } from "react";
import {
  Box,
  Checkbox,
  FormControlLabel,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Typography,
} from "@mui/material";
import type { TableData } from "./tables";

export function DataTable({ data }: { data: TableData }) {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(24);
  const rows = data.rows.slice(page * rowsPerPage, (page + 1) * rowsPerPage);
  return (
    <Box sx={{ mt: 2 }}>
      <TableContainer sx={{ maxHeight: 420 }} tabIndex={0} aria-label={`Scrollable table: ${data.caption}`} role="region">
        <Table size="small" stickyHeader aria-label={data.caption}>
          <caption>{data.caption}</caption>
          <TableHead>
            <TableRow>
              {data.columns.map((c) => (
                <TableCell key={c} scope="col">{c}</TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map((r, i) => (
              <TableRow key={page * rowsPerPage + i}>
                {r.map((cell, j) => (
                  j === 0 ? <TableCell key={j} component="th" scope="row">{cell}</TableCell> : <TableCell key={j}>{cell}</TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {data.rows.length > 12 && (
        <TablePagination
          component="div"
          count={data.rows.length}
          page={page}
          onPageChange={(_, p) => setPage(p)}
          rowsPerPage={rowsPerPage}
          rowsPerPageOptions={[12, 24, 48]}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(Number(e.target.value));
            setPage(0);
          }}
        />
      )}
    </Box>
  );
}

interface Props {
  title: string;
  subtitle?: string;
  controls?: ReactNode;
  /** Built only when the user opens the table. */
  table: () => TableData;
  children: ReactNode;
}

export function ChartCard({ title, subtitle, controls, table, children }: Props) {
  const id = useId();
  const [showTable, setShowTable] = useState(false);
  return (
    <Paper component="section" aria-labelledby={id} sx={{ p: 2, mb: 3 }}>
      <Typography id={id} variant="h2" sx={{ fontSize: "1.3rem" }}>{title}</Typography>
      {subtitle && <Typography variant="body2" color="text.secondary">{subtitle}</Typography>}
      {controls && <Box sx={{ display: "flex", gap: 2, flexWrap: "wrap", alignItems: "center", my: 1 }}>{controls}</Box>}
      {children}
      <FormControlLabel control={<Checkbox checked={showTable} onChange={(e) => setShowTable(e.target.checked)} />} label="Show data table" />
      {showTable && <DataTable data={table()} />}
    </Paper>
  );
}
