import type { Metadata } from "next";
import "./styles.css";

export const metadata: Metadata = {
  title: "Converge",
  description: "Fault-tolerant visual collaboration",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>): React.JSX.Element {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
