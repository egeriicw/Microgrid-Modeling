export type ZoomAction = "in" | "out" | "left" | "right" | "reset";
export type ZoomWindow = [start: number, end: number];

const MIN_WIDTH = 0.05; // percent of the year (about 26 minutes of 35,040 points)

const clampWindow = (start: number, end: number): ZoomWindow => {
  const width = Math.min(100, end - start);
  const s = Math.min(Math.max(0, start), 100 - width);
  return [s, s + width];
};

/** Pure keyboard/button zoom maths on the 0..100 percent window shown by the dataZoom component. */
export function zoomWindow([start, end]: ZoomWindow, action: ZoomAction): ZoomWindow {
  const width = end - start;
  const centre = (start + end) / 2;
  switch (action) {
    case "reset":
      return [0, 100];
    case "in": {
      const w = Math.max(MIN_WIDTH, width / 2);
      return clampWindow(centre - w / 2, centre + w / 2);
    }
    case "out": {
      const w = Math.min(100, width * 2);
      return clampWindow(centre - w / 2, centre + w / 2);
    }
    case "left":
      return clampWindow(start - width / 4, end - width / 4);
    case "right":
      return clampWindow(start + width / 4, end + width / 4);
  }
}
