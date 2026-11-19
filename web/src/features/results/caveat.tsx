import { Alert } from "@mui/material";

// FR-WEB-8: one wording source for every place the accuracy caveat appears (results view and run screen).
export const ACCURACY_CAVEAT = "Methodology demonstration with benchmark checks — not validated for engineering or investment decisions.";

/** The caveat; `compact` = a one-line form for the run screen. Kept free of chart code so it loads anywhere. */
export function AccuracyCaveat({ compact = false }: { compact?: boolean }) {
  return (
    <Alert role="note" severity="warning" sx={compact ? { py: 0, my: 1, "& .MuiAlert-message": { py: 0.75 } } : { mb: 2 }}>
      {ACCURACY_CAVEAT}
    </Alert>
  );
}
