import { Box, Typography } from "@mui/material";
import type { DataFiles } from "../../api/types";

const NOT_RECORDED = "not recorded for this run";

function unitsText(units: string | null): string {
  if (units === "C") return "degrees Celsius";
  if (units === "F") return "degrees Fahrenheit (converted to Celsius in the results)";
  return "units not recorded";
}

/** One plain sentence about where the outdoor temperature came from (the server's declaration, not a guess). */
export function temperatureSourceText(t: DataFiles["temperature"]): string {
  switch (t.source) {
    case null:
      return `Outdoor temperature: ${NOT_RECORDED}.`;
    case "none":
      return "Outdoor temperature: no temperature source was set up for this run, so the results have no temperature.";
    case "timeseries_column":
      return `Outdoor temperature: taken from each sampled building's own load data, in ${unitsText(t.units)}.`;
    case "weather_file":
      return `Outdoor temperature: taken from one weather file (file ${t.file ?? "not recorded"}), in ${unitsText(t.units)}.`;
    default:
      return "Outdoor temperature: from a source this version of the web app cannot describe (see technical details).";
  }
}

const fileText = (name: string | null) => (name ? `(file ${name})` : `(file ${NOT_RECORDED})`);

/** FR-DATA-8 provenance in plain language; raw source and column names only in the collapsed technical details. */
export function DataUsedPanel({ files }: { files: DataFiles }) {
  const t = files.temperature;
  const lines = [
    `Homes: NREL ResStock building characteristics ${fileText(files.resstockCharacteristicsFile)}`,
    `Commercial buildings: NREL ComStock building characteristics ${fileText(files.comstockCharacteristicsFile)}`,
    temperatureSourceText(t),
  ];
  const technical = [
    `Temperature source: ${t.source ?? NOT_RECORDED}`,
    ...(t.file ? [`Weather file: ${t.file}`] : []),
    ...(t.column ? [`Temperature column: ${t.column}`] : []),
    ...(t.timeColumn ? [`Time column: ${t.timeColumn}`] : []),
    ...(t.units ? [`Declared units: ${t.units}`] : []),
  ];
  return (
    <Box component="section" aria-labelledby="data-used-h" sx={{ mt: 3 }}>
      <Typography id="data-used-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Data used</Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>The data on the server this run read.</Typography>
      <Box component="ul" sx={{ m: 0, pl: 3 }}>
        {lines.map((l) => (
          <Typography key={l} component="li" variant="body2" sx={{ overflowWrap: "anywhere" }}>{l}</Typography>
        ))}
      </Box>
      <Box component="details" sx={{ mt: 1 }}>
        <Typography component="summary" variant="body2" sx={{ cursor: "pointer", py: 1.5 /* about 44 px tall: a touch-sized target; display stays list-item so the marker shows */ }}>
          Technical details
        </Typography>
        <Box component="ul" sx={{ m: 0, pl: 3 }}>
          {technical.map((l) => (
            <Typography key={l} component="li" variant="body2" sx={{ overflowWrap: "anywhere" }}>{l}</Typography>
          ))}
        </Box>
      </Box>
    </Box>
  );
}
