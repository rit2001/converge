import * as React from "react";
import { cloneElement, useId, type ComponentPropsWithoutRef, type ReactElement } from "react";

function classes(...values: Array<string | false | null | undefined>): string {
  return values.filter(Boolean).join(" ");
}

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";

export interface ButtonProps extends ComponentPropsWithoutRef<"button"> {
  variant?: ButtonVariant;
  size?: "default" | "icon";
  loading?: boolean;
}

export function Button({
  variant = "secondary",
  size = "default",
  loading = false,
  disabled,
  className,
  children,
  type = "button",
  ...props
}: ButtonProps): React.JSX.Element {
  return (
    <button
      {...props}
      type={type}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      data-loading={loading || undefined}
      className={classes("ui-button", `ui-button--${variant}`, `ui-button--${size}`, className)}
    >
      {loading && <span className="ui-button__spinner" aria-hidden="true" />}
      {children}
    </button>
  );
}

export interface IconButtonProps extends Omit<ButtonProps, "size"> {
  "aria-label": string;
}

export function IconButton({ "aria-label": label, ...props }: IconButtonProps): React.JSX.Element {
  if (!label.trim()) throw new Error("IconButton requires a nonempty accessible name");
  return <Button {...props} size="icon" aria-label={label} />;
}

export type BadgeTone =
  | "neutral"
  | "success"
  | "warning"
  | "danger"
  | "information"
  | "reconnecting"
  | "recovering"
  | "unavailable"
  | "revoked";

export interface StatusPillProps extends ComponentPropsWithoutRef<"div"> {
  label: string;
  tone?: BadgeTone;
  accessibleLabel?: string;
}

export function StatusPill({
  label,
  tone = "neutral",
  accessibleLabel,
  className,
  ...props
}: StatusPillProps): React.JSX.Element {
  return (
    <div
      {...props}
      role="status"
      aria-label={accessibleLabel ?? label}
      className={classes("ui-status-pill", `ui-status-pill--${tone}`, className)}
    >
      <span className="ui-status-pill__indicator" aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}

export function Surface({
  className,
  ...props
}: ComponentPropsWithoutRef<"section">): React.JSX.Element {
  return <section {...props} className={classes("ui-surface", className)} />;
}

export function Separator({
  className,
  ...props
}: ComponentPropsWithoutRef<"hr">): React.JSX.Element {
  return <hr {...props} className={classes("ui-separator", className)} />;
}

export function VisuallyHidden({
  className,
  ...props
}: ComponentPropsWithoutRef<"span">): React.JSX.Element {
  return <span {...props} className={classes("ui-visually-hidden", className)} />;
}

export function Tooltip({
  label,
  children,
}: {
  label: string;
  children: ReactElement<{ "aria-describedby"?: string }>;
}): React.JSX.Element {
  const id = useId();
  if (!label.trim()) throw new Error("Tooltip requires nonempty text");
  return (
    <span className="ui-tooltip">
      {cloneElement(children, { "aria-describedby": id })}
      <span id={id} role="tooltip" className="ui-tooltip__content">
        {label}
      </span>
    </span>
  );
}
