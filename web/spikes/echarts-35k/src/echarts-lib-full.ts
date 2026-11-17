// Used only to measure the bundle size of the full `echarts` import (BUNDLE=full).
import * as echarts from "echarts";

export const init = echarts.init;
export type Chart = ReturnType<typeof echarts.init>;
