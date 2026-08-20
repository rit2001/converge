import type { Metadata } from "next";
import { Workspace } from "../../src/components/workspace";

export const metadata: Metadata = {
  title: "Studio — Converge",
  description: "The Converge collaborative visual workspace.",
};

export default function StudioPage(): React.JSX.Element {
  return <Workspace />;
}
