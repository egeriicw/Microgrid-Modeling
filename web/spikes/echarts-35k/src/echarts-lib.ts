// Tree-shaken ECharts: only what the spike uses. This is the import style to use in the real app.
import * as echarts from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import {
  AriaComponent,
  DataZoomComponent,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  LineChart,
  ScatterChart,
  AriaComponent,
  DataZoomComponent,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  CanvasRenderer,
]);

export const init = echarts.init;
export type Chart = ReturnType<typeof echarts.init>;
