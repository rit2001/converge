import Link from "next/link";
import * as React from "react";

const evidence = [
  ["10", "concurrent active editors"],
  ["1,300", "acknowledged durable commands"],
  ["227.03 ms", "p99 acknowledgement"],
  ["368 ms", "p99 live delivery"],
] as const;

export function LandingPage(): React.JSX.Element {
  return (
    <div className="landing-page">
      <a className="skip-link" href="#main-content">
        Skip to content
      </a>
      <header className="landing-header">
        <Link className="landing-wordmark" href="/" aria-label="Converge home">
          <span className="landing-wordmark__mark" aria-hidden="true">
            C
          </span>
          <span>Converge</span>
        </Link>
        <nav aria-label="Primary navigation">
          <a href="#trust">Why Converge</a>
          <a href="#evidence">Evidence</a>
          <Link className="ui-button ui-button--primary ui-button--default" href="/studio">
            Open the studio
          </Link>
        </nav>
      </header>

      <main id="main-content">
        <section className="landing-hero" aria-labelledby="landing-title">
          <div className="landing-hero__copy">
            <p className="landing-eyebrow">A calm shared studio</p>
            <h1 id="landing-title">Shared thinking that survives the network.</h1>
            <p className="landing-hero__lede">
              Shape ideas together with collaboration that stays ordered, preserves durable work,
              and recovers carefully after interruption.
            </p>
            <div className="landing-actions">
              <Link className="ui-button ui-button--primary ui-button--default" href="/studio">
                Open the studio
              </Link>
              <a className="ui-button ui-button--secondary ui-button--default" href="#trust">
                See how it stays in sync
              </a>
            </div>
            <p className="landing-hero__note">
              Desktop-first editor preview. No board is created until you open the studio.
            </p>
          </div>

          <figure className="product-illustration" aria-hidden="true">
            <div className="product-illustration__bar">
              <span className="product-illustration__brand">Converge</span>
              <span className="product-illustration__status">
                <i /> In sync
              </span>
              <span className="product-illustration__people">AR · MK</span>
            </div>
            <div className="product-illustration__canvas">
              <div className="illustration-note illustration-note--one">
                <strong>Launch story</strong>
                <span>Make the sequence feel inevitable.</span>
              </div>
              <div className="illustration-shape illustration-shape--selection">
                <span>Trust first</span>
                <i className="illustration-handle illustration-handle--one" />
                <i className="illustration-handle illustration-handle--two" />
                <i className="illustration-handle illustration-handle--three" />
                <i className="illustration-handle illustration-handle--four" />
              </div>
              <div className="illustration-note illustration-note--two">
                <strong>Recovered</strong>
                <span>Back in sync without hiding the interruption.</span>
              </div>
              <div className="illustration-cursor illustration-cursor--one">
                <span /> Ava
              </div>
              <div className="illustration-cursor illustration-cursor--two">
                <span /> Malik
              </div>
              <div className="illustration-layers">
                <b>Layers</b>
                <span>Launch story</span>
                <span>Trust first</span>
                <span>Recovered</span>
              </div>
            </div>
            <figcaption>Static illustration of the Converge workspace</figcaption>
          </figure>
        </section>

        <section className="landing-trust" id="trust" aria-labelledby="trust-title">
          <div className="landing-section-heading">
            <p className="landing-eyebrow">Trust is a product feature</p>
            <h2 id="trust-title">Collaboration should explain itself when conditions change.</h2>
          </div>
          <div className="trust-principles">
            <article>
              <span className="principle-number">01</span>
              <h3>Ordered collaboration</h3>
              <p>Everyone applies accepted changes in the same authoritative board sequence.</p>
            </article>
            <article>
              <span className="principle-number">02</span>
              <h3>Careful recovery</h3>
              <p>
                After interruption, Converge verifies a snapshot and contiguous history before
                editing resumes.
              </p>
            </article>
            <article>
              <span className="principle-number">03</span>
              <h3>Durable work</h3>
              <p>
                Committed work lives in PostgreSQL; a fleeting socket connection is never the source
                of truth.
              </p>
            </article>
          </div>
        </section>

        <section className="landing-evidence" id="evidence" aria-labelledby="evidence-title">
          <div className="landing-evidence__intro">
            <p className="landing-eyebrow">Measured, with context</p>
            <h2 id="evidence-title">
              A controlled local baseline—not a production capacity claim.
            </h2>
            <p>
              One 10-editor, two-minute run completed with zero protocol failures, sequence gaps, or
              logical reapplications. It demonstrates the tested workflow, not a scaling ceiling.
            </p>
          </div>
          <dl className="evidence-grid">
            {evidence.map(([value, label]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        </section>

        <section className="landing-final" aria-labelledby="final-title">
          <p className="landing-eyebrow">Ready when you are</p>
          <h2 id="final-title">
            Bring the room together without pretending the network is perfect.
          </h2>
          <Link className="ui-button ui-button--primary ui-button--default" href="/studio">
            Open the studio
          </Link>
        </section>
      </main>

      <footer className="landing-footer">
        <span>Converge</span>
        <nav aria-label="Footer navigation">
          <a href="#main-content">Back to top</a>
          <Link href="/studio">Studio</Link>
        </nav>
      </footer>
    </div>
  );
}
