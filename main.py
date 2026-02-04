#!/usr/bin/env python3
"""
Polymarket Account Data Collector and Analyzer

Usage:
    python main.py fetch --profile-url "https://polymarket.com/@gabagool22" --out ./out/gabagool22
    python main.py fetch --handle "gabagool22" --out ./out/gabagool22
    python main.py report --in ./out/gabagool22
"""
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from polymarket_collector import UserDataCollector, ReportAnalyzer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Polymarket Account Data Collector and Analyzer.

    Fetch trading data from Polymarket accounts and generate detailed analysis reports.
    """
    pass


@cli.command()
@click.option(
    "--profile-url",
    type=str,
    default=None,
    help="Full Polymarket profile URL (e.g., https://polymarket.com/@gabagool22)",
)
@click.option(
    "--handle",
    type=str,
    default=None,
    help="Polymarket handle without @ (e.g., gabagool22)",
)
@click.option(
    "--wallet",
    type=str,
    default=None,
    help="Wallet address directly (e.g., 0x6ac5bb06...)",
)
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=Path("./out"),
    help="Output directory for data files",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging (DEBUG level)",
)
def fetch(
    profile_url: Optional[str],
    handle: Optional[str],
    wallet: Optional[str],
    out: Path,
    verbose: bool,
):
    """Fetch all trading data for a Polymarket user.

    Examples:
        python main.py fetch --profile-url "https://polymarket.com/@gabagool22"
        python main.py fetch --handle "gabagool22" --out ./data/gabagool22
        python main.py fetch --wallet "0x6ac5bb06a9eb05641fd5e82640268b92f3ab4b6e"
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not profile_url and not handle and not wallet:
        click.echo("Error: Either --profile-url, --handle, or --wallet must be provided", err=True)
        sys.exit(1)

    click.echo(f"🔍 Starting data collection...")
    click.echo(f"📁 Output directory: {out}")

    try:
        # Run async collection
        collector = UserDataCollector(out)
        metadata = asyncio.run(
            collector.collect_all_data(
                profile_url=profile_url,
                handle=handle,
                wallet=wallet,
            )
        )

        # Display summary
        click.echo("\n✅ Data collection completed!")
        click.echo(f"\n📊 Summary:")
        click.echo(f"  - Trades: {metadata.get('total_trades', 0):,}")
        click.echo(f"  - Activities: {metadata.get('total_activities', 0):,}")
        click.echo(f"  - Open Positions: {metadata.get('total_positions', 0):,}")
        click.echo(f"  - Closed Positions: {metadata.get('total_closed_positions', 0):,}")

        if metadata.get("errors"):
            click.echo(f"\n⚠️  Errors encountered: {len(metadata['errors'])}")
            for error in metadata["errors"]:
                click.echo(f"  - {error}", err=True)

        # Get the actual output directory (may be nested under handle)
        actual_out = Path(out) / metadata.get("handle", "")
        if actual_out.exists():
            out = actual_out

        click.echo(f"\n💾 Data saved to: {out}")
        click.echo(f"\nNext step: Generate report with:")
        click.echo(f"  python main.py report --in {out}")

    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Collection interrupted by user", err=True)
        sys.exit(130)
    except Exception as e:
        click.echo(f"\n❌ Error: {e}", err=True)
        if verbose:
            logger.exception("Collection failed")
        sys.exit(1)


@cli.command()
@click.option(
    "--in",
    "input_dir",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Input directory containing collected data",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output file for report (default: <input_dir>/report.md)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging (DEBUG level)",
)
def report(
    input_dir: Path,
    output: Optional[Path],
    verbose: bool,
):
    """Generate analysis report from collected data.

    Examples:
        python main.py report --in ./out/gabagool22
        python main.py report --in ./data/gabagool22 --output ./report.md
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo(f"📊 Generating report from: {input_dir}")

    try:
        # Generate report
        analyzer = ReportAnalyzer(input_dir)
        report_path = analyzer.save_report()

        # Read and display report
        with open(report_path, "r") as f:
            report_content = f.read()

        click.echo("\n" + "="*80)
        click.echo(report_content)
        click.echo("="*80)

        click.echo(f"\n✅ Report saved to: {report_path}")

        # Copy to custom output location if specified
        if output:
            output = Path(output)
            output.parent.mkdir(parents=True, exist_ok=True)
            with open(output, "w") as f:
                f.write(report_content)
            click.echo(f"📄 Report also saved to: {output}")

    except Exception as e:
        click.echo(f"\n❌ Error: {e}", err=True)
        if verbose:
            logger.exception("Report generation failed")
        sys.exit(1)


@cli.command()
@click.option(
    "--profile-url",
    type=str,
    default=None,
    help="Full Polymarket profile URL",
)
@click.option(
    "--handle",
    type=str,
    default=None,
    help="Polymarket handle without @",
)
@click.option(
    "--wallet",
    type=str,
    default=None,
    help="Wallet address directly (e.g., 0x6ac5bb06...)",
)
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=Path("./out"),
    help="Output directory for data files",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose logging",
)
def run(
    profile_url: Optional[str],
    handle: Optional[str],
    wallet: Optional[str],
    out: Path,
    verbose: bool,
):
    """Fetch data and generate report in one step.

    Examples:
        python main.py run --handle "gabagool22"
        python main.py run --profile-url "https://polymarket.com/@gabagool22" --out ./data
        python main.py run --wallet "0x6ac5bb06a9eb05641fd5e82640268b92f3ab4b6e"
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # First, fetch data
    ctx = click.get_current_context()
    ctx.invoke(fetch, profile_url=profile_url, handle=handle, wallet=wallet, out=out, verbose=verbose)

    # Determine the actual output directory
    if wallet:
        actual_out = out / wallet.lower().strip()[:10]
    elif handle:
        actual_out = out / handle
    elif profile_url:
        # Extract handle from URL
        import re
        match = re.search(r"@([^/?]+)", profile_url)
        if match:
            actual_out = out / match.group(1)
        else:
            actual_out = out
    else:
        actual_out = out

    # Then, generate report
    click.echo("\n" + "="*80 + "\n")
    ctx.invoke(report, input_dir=actual_out, output=None, verbose=verbose)


if __name__ == "__main__":
    cli()