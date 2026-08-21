import type { Metadata, Viewport } from "next";
import { PortalRoots } from "../src/components/ui/portal-roots";
import { ThemeProvider } from "../src/theme-provider";
import "./styles.css";

export const metadata: Metadata = {
  title: "Converge",
  description: "Fault-tolerant visual collaboration",
};

export const viewport: Viewport = {
  colorScheme: "light dark",
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#eef0f5" },
    { media: "(prefers-color-scheme: dark)", color: "#171922" },
  ],
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>): React.JSX.Element {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <script
          dangerouslySetInnerHTML={{
            __html:
              '(function(){try{var r=localStorage.getItem("converge:theme:v1"),p="system";if(r){var v=JSON.parse(r);if(v&&v.version===1&&(v.preference==="system"||v.preference==="light"||v.preference==="dark"))p=v.preference}var d=p==="system"&&(matchMedia("(prefers-color-scheme: dark)").matches)?"dark":p==="system"?"light":p;document.documentElement.dataset.theme=d;document.documentElement.dataset.themePreference=p;document.documentElement.style.colorScheme=d}catch(e){}})();',
          }}
        />
        <ThemeProvider>
          <div id="application-root">{children}</div>
          <PortalRoots />
        </ThemeProvider>
      </body>
    </html>
  );
}
