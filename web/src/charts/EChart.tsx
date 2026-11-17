import { useEffect, useRef } from "react";
import { BarChart, LineChart } from "echarts/charts";
import { AriaComponent, DataZoomComponent, DatasetComponent, GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { init, use as registerEcharts, type EChartsCoreOption } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

// Tree-shaken ECharts: only what the charts use (see docs/research/echarts-spike.md section 3.4).
registerEcharts([LineChart, BarChart, AriaComponent, DataZoomComponent, DatasetComponent, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

/** The slice of the chart API the app relies on; tests supply a fake. */
export interface ChartLike {
  dispatchAction(payload: { type: "dataZoom"; start: number; end: number }): void;
  on(event: string, handler: () => void): void;
  off(event: string, handler?: () => void): void;
  getOption(): { dataZoom?: { start: number; end: number }[] };
  getZr(): {
    on(name: string, h: (e: { offsetX: number; offsetY: number }) => void): void;
    off(name: string, h: (e: { offsetX: number; offsetY: number }) => void): void;
  };
  containPixel(finder: string, pt: number[]): boolean;
  convertFromPixel(finder: { xAxisIndex: number }, v: number): number | number[];
}

export interface EChartProps {
  option: EChartsCoreOption;
  ariaLabel: string;
  height?: number;
  onReady?: (chart: ChartLike) => void;
}

export function EChart({ option, ariaLabel, height = 420, onReady }: EChartProps) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<ReturnType<typeof init> | null>(null);
  const ready = useRef(onReady);
  ready.current = onReady;

  useEffect(() => {
    const c = init(box.current!, undefined, { renderer: "canvas" });
    chart.current = c;
    ready.current?.(c as unknown as ChartLike);
    const ro = new ResizeObserver(() => c.resize());
    ro.observe(box.current!);
    return () => {
      ro.disconnect();
      c.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.setOption(option, true);
  }, [option]);

  return <div ref={box} role="img" aria-label={ariaLabel} style={{ width: "100%", height, position: "relative" }} />;
}
