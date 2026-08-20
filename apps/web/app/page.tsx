import type { Metadata } from "next";
import * as React from "react";
import { LandingPage } from "../src/components/landing-page";

const description =
  "A calm shared studio for visual collaboration that stays ordered, durable, and recoverable.";

export const metadata: Metadata = {
  title: "Converge — shared thinking that survives the network",
  description,
  openGraph: {
    title: "Converge — shared thinking that survives the network",
    description,
    type: "website",
  },
  twitter: {
    card: "summary",
    title: "Converge — shared thinking that survives the network",
    description,
  },
};

export default function Page(): React.JSX.Element {
  return <LandingPage />;
}
