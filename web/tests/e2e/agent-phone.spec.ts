import { readFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test, type Locator, type Page } from "@playwright/test";
import { mockApi } from "./agent-mocks";

const FIXTURE = readFileSync(join(__dirname, "../fixtures/agent-best-tippers.sse"), "utf8");

const noSideScroll = (page: Page) => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth);
const top = async (l: Locator) => (await l.boundingBox())!.y;

// Runs on the iPhone 13 and Pixel 7 projects (playwright.config.ts). ADR-025.
test("on a phone: 16 px base, one-line header badge, no sideways scroll", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.goto("/agent");
  await expect(page.getByText("Thu 09 Oct 17:42")).toBeVisible();
  expect(await page.evaluate(() => getComputedStyle(document.documentElement).fontSize)).toBe("16px");
  const badge = (await page.getByText("local model").boundingBox())!;
  expect(badge.height).toBeLessThan(30); // one line, not a wrapped blob
  expect(await noSideScroll(page)).toBe(true);
});

test("on a phone: the answer comes right after the form, then steps, SQL, data and the suggestions", async ({ page }) => {
  await mockApi(page, FIXTURE);
  await page.setViewportSize({ width: 360, height: 740 }); // the narrowest phone spec 08 supports
  await page.goto("/agent");
  await page.getByRole("button", { name: "Which borough has the best tippers?" }).click();

  const answer = page.getByTestId("answer");
  await expect(answer).toContainText("Manhattan riders tip best: 23.1%");
  await expect(answer).toBeInViewport();
  const form = page.getByRole("textbox", { name: "Your question" });
  const order = [form, answer, page.getByTestId("steps"), page.getByTestId("sql-panel"), page.getByRole("table"),
    page.getByRole("navigation", { name: "Suggested questions" })];
  const ys = await Promise.all(order.map(top));
  expect(ys).toEqual([...ys].sort((a, b) => a - b));

  // Bars run horizontally on a phone: one row per borough, each name a whole, readable label.
  const label = (name: string) => page.locator(".recharts-cartesian-axis-tick-value", { hasText: name });
  await expect(label("Staten Island")).toBeVisible();
  const [manhattan, staten] = [(await label("Manhattan").boundingBox())!, (await label("Staten Island").boundingBox())!];
  expect(staten.y).toBeGreaterThan(manhattan.y + manhattan.height);
  expect(Math.abs(staten.x + staten.width - (manhattan.x + manhattan.width))).toBeLessThan(2); // right-aligned to the axis
  expect(await noSideScroll(page)).toBe(true);
});
