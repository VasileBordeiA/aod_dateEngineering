/** The WIFI: QR format phones understand. \ ; , : " must be backslash-escaped in SSID and password. */
const esc = (s: string): string => s.replace(/([\\;,:"])/g, "\\$1");

export function wifiQrPayload(ssid: string, password: string): string {
  return password ? `WIFI:T:WPA;S:${esc(ssid)};P:${esc(password)};;` : `WIFI:T:nopass;S:${esc(ssid)};;`;
}
