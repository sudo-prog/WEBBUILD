# Hermes Agent & Kilo Code Collaboration Guide

## Overview
This document outlines how Hermes Agent (AI assistant) and Kilo Code (VS Code extension) can work together to maintain and enhance the Supabase Australia pipeline.

## Roles & Responsibilities

### Kilo Code (VS Code Extension)
- **Primary Pipeline Maintenance:** Run daily cron jobs, manage data extraction
- **Code Editing:** Make structural changes to scripts with AI assistance
- **Local Development:** Test changes in the development environment
- **Web Dashboard Management:** Serve and maintain the lead generator interface

### Hermes Agent (AI Assistant)
- **Monitoring & Alerts:** Check system health, detect anomalies
- **Troubleshooting:** Diagnose and fix issues across the stack
- **Automation:** Set up and manage cron jobs, backups, monitoring
- **Knowledge Management:** Maintain documentation, create handover packages
- **Integration:** MCP setup, API key management, credential handling

## Collaboration Scenarios

### 1. Daily Operations
- **Kilo Code:** Runs daily pipeline via cron job
- **Hermes Agent:** Monitors health, checks logs, sends alerts if issues detected
- **Handover:** Hermes provides daily summary of system status

### 2. Issue Resolution
- **Trigger:** Pipeline fails or data quality issues detected
- **Process:**
  1. Hermes detects issue via monitoring or error alerts
  2. Hermes diagnoses problem and suggests fixes
  3. Kilo Code implements fix in VS Code with AI assistance
  4. Hermes verifies fix and updates documentation
- **Fallback:** Hermes can directly fix certain issues and commit changes

### 3. System Enhancements
- **Idea:** Add new features or improve pipeline
- **Process:**
  1. User discusses requirements with Hermes
  2. Hermes creates implementation plan
  3. Kilo Code executes development with AI guidance
  4. Hermes tests changes and updates monitoring
- **Documentation:** Both systems update documentation accordingly

### 4. Monitoring & Alerts
- **Hermes Agent:** 
  - Runs daily health checks
  - Monitors database size and performance
  - Tracks pipeline success rates
  - Sends alerts for anomalies
- **Kilo Code:**
  - Implements monitoring scripts
  - Maintains dashboard visualizations
  - Sets up notification channels

## Communication Channels

### 1. MCP (Model Context Protocol)
- **Configuration:** Already set up in ~/.vscode/mcp.json
- **Usage:** Hermes can communicate with VS Code and Kilo Code via MCP
- **Benefits:** Seamless context sharing, tool access, coordinated actions

### 2. Direct Tasks
- User can assign tasks to Hermes via chat interface
- Hermes can create subagents for parallel work
- Hermes can delegate to Kilo Code for code-specific tasks

### 3. Documentation
- **AGENTS_NOTES.md:** Project history and decisions
- **HANDOVER_TO_KILO.md:** Technical specifications and setup
- **COLLABORATION_GUIDE.md:** Working relationships and processes

## Getting Started with Hermes Agent

### Basic Commands
```bash
# Ask for help or information
hermes: How do I check the database connection?

# Request a task
hermes: Please run a health check on the Supabase pipeline.

# Delegate a coding task
hermes: Can you fix the phone normalization in pipeline_fixed.py?
```

### Common Tasks Hermes Can Perform
- ✅ Run health checks and diagnostics
- ✅ Fix bugs and implement small features
- ✅ Set up monitoring and alerts
- ✅ Manage cron jobs
- ✅ Create and update documentation
- ✅ Troubleshoot system issues
- ✅ Integrate with MCP and other tools

### When to Use Kilo Code vs. Hermes Agent
- **Use Kilo Code for:** Daily pipeline operations, code editing, local testing, VS Code integration
- **Use Hermes Agent for:** Monitoring, alerting, troubleshooting, automation, documentation, system-wide tasks

## Example Workflow: Daily Health Check

### Morning Routine
1. **Hermes Agent** (automatically at 12:00 PM):
   - Runs `monitor_health.sh`
   - Checks database connectivity
   - Verifies lead count
   - Confirms backups exist
   - Logs results to `monitor_health.log`
   - Sends summary if issues found

2. **Kilo Code** (if issues detected):
   - Opens VS Code
   - Reviews Hermes's findings
   - Implements fix with AI assistance
   - Tests solution
   - Notifies team of resolution

### Evening Routine
1. **Hermes Agent** (automatically at 2:00 AM):
   - Triggers pipeline run via cron
   - Verifies completion via ingestion_log

2. **Hermes Agent** (automatically at 3:00 AM):
   - Runs database backup
   - Cleans up old backups

## Escalation Path

### Level 1: Automated Monitoring
- Hermes daily checks
- Email/SMS alerts for critical issues

### Level 2: AI Assistance
- Kilo Code in VS Code for code fixes
- Hermes for diagnostics and planning

### Level 3: Human Intervention
- Complex architectural changes
- Manual data recovery
- System upgrades

## Success Metrics

- **Pipeline Uptime:** 99.9% (target)
- **Data Freshness:** Daily updates by 3:00 AM
- **Backup Reliability:** 100% daily backups retained
- **Issue Resolution:** < 2 hours for critical issues
- **Lead Quality:** 95%+ pass validation

---

**Last Updated:** 2026-05-05  
**Next Review:** Weekly  
**Owner:** Hermes Agent & Kilo Code