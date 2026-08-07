import http from "k6/http";
import { check } from "k6";

export const options = { vus: 1, iterations: 1 };
export default function smoke() {
  const response = http.get(`${__ENV.API_URL ?? "http://localhost:4000"}/health`);
  check(response, { "health is 200": (result) => result.status === 200 });
}
