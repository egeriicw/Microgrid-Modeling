import { Box, Typography } from "@mui/material";
import type { DataFiles } from "../../api/types";

const NOT_RECORDED = "not recorded for this run";

function unitsText(units: string | null): string {
  if (units === "C") return "degrees Celsius";
  if (units === "F") return "degrees Fahrenheit (converted to Celsius in the results)";
  return units ? `units ${units}` : "units not recorded";
}

/** Plain-language description of the temperature source the run declared (the server's declaration, not a guess). */
export function temperatureSourceText(t: DataFiles["temperature"]): string {
  switch (t.source) {
    case null:
      return `Outdoor temperature: ${NOT_RECORDED}.`;
    case "none":
      return "Outdoor temperature: no outdoor temperature source was declared for this run, so the results have no temperature.";
    case "timeseries_column":
      return `Outdoor temperature: read from column ${t.column ?? "(not recorded)"} of each sampled building's load file, in ${unitsText(t.units)}.`;
    case "weather_file":
      return `Outdoor temperature: read from the weather file ${t.file ?? "(not recorded)"} (column ${t.column ?? "(not recorded)"}, time column ${t.timeColumn ?? "(not recorded)"}), in ${unitsText(t.units)}.`;
    default:
      return `Outdoor temperature: source "${t.source}" (not described by this version of the web app).`;
  }
}

/** FR-DATA-8 provenance: which files a run read, by name only (the server never sends paths). Text only. */
export function DataUsedPanel({ files }: { files: DataFiles }) {
  const lines = [
    `Home (ResStock) characteristics: ${files.resstockCharacteristicsFile ?? NOT_RECORDED}`,
    `Building (ComStock) characteristics: ${files.comstockCharacteristicsFile ?? NOT_RECORDED}`,
    temperatureSourceText(files.temperature),
  ];
  return (
    <Box component="section" aria-labelledby="data-used-h" sx={{ mt: 3 }}>
      <Typography id="data-used-h" variant="h2" sx={{ fontSize: "1.2rem", mb: 1 }}>Data used</Typography>
      <Typography variant="body2" sx={{ mb: 1 }}>The data files on the server this run read (file names only).</Typography>
      <Box component="ul" sx={{ m: 0, pl: 3 }}>
        {lines.map((l) => (
          <Typography key={l} component="li" variant="body2" sx={{ overflowWrap: "anywhere" }}>{l}</Typography>
        ))}
      </Box>
    </Box>
  );
}
