import { expect, it } from "vitest";
import { wifiQrPayload } from "@/lib/wifi";

it("WPA payload with special characters escaped", () => {
  expect(wifiQrPayload("NYC-Taxi-Live", "p@ss")).toBe("WIFI:T:WPA;S:NYC-Taxi-Live;P:p@ss;;");
  expect(wifiQrPayload('My;Net,"x"', "a:b\\c")).toBe('WIFI:T:WPA;S:My\\;Net\\,\\"x\\";P:a\\:b\\\\c;;');
});

it("an open network has no password field", () => {
  expect(wifiQrPayload("Open", "")).toBe("WIFI:T:nopass;S:Open;;");
});
