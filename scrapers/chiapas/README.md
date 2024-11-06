# Fiscalia Chiapas Missing Persons Scraper

## Purpose

This scraper is designed to collect and analyze information about missing persons from the Fiscalía General del Estado de Chiapas (State Attorney General's Office of Chiapas) website. The primary goal is to create a comprehensive database of missing persons cases to:

- Track and monitor missing persons cases in Chiapas
- Enable data analysis for identifying patterns and trends
- Support transparency and accountability in missing persons investigations
- Facilitate research and advocacy efforts

## Technical Architecture

### ARM64 Support

The project is specifically designed to run on ARM64 architecture (like M1/M2 Macs) using:

- `seleniarm/standalone-chromium` for the Selenium environment
- `condaforge/miniforge3` as the base image for ARM compatibility
- Platform-specific optimizations in the Docker configuration

### Components

1. **Selenium Hub**

   - Standalone Chromium instance for web automation
   - Configured with VNC support for debugging
   - Optimized for ARM64 architecture
2. **Scraper Container**

   - Built on Miniconda for efficient package management
   - Jupyter Lab interface for development and monitoring
   - OCR capabilities with Tesseract (Spanish language support)
   - Custom Python environment with all required dependencies

### Local Development Setup

1. **Prerequisites**

   ```bash
   cd scrapers/fiscalia-chiapas
   ```
2. **Environment Setup**

   ```bash
   docker-compose up
   ```
3. **VSCode Connection**

   - Open VSCode
   - Navigate to the Remote Explorer extension
   - Connect to `localhost:8888` for Jupyter Lab access
   - Alternative: Open `http://localhost:8888` in your browser

## Current Features

- Automated navigation through the Fiscalía's website
- Data extraction from missing persons records
- OCR processing for image-based data
- Structured data storage in standardized format
- Error handling and retry mechanisms
- Session management and rate limiting

## Parallelization Opportunities

The scraper can be optimized for faster data collection through:

1. **Multiple Browser Sessions**

   - Configurable number of concurrent Selenium sessions
   - Load balancing across sessions
   - Session pooling for efficient resource usage
2. **Distributed Processing**

   - Queue-based task distribution
   - Independent processing of different date ranges
   - Parallel OCR processing
3. **Batch Processing**

   - Grouping requests to minimize network overhead
   - Bulk data storage operations
   - Concurrent data transformation pipelines

## Future Extensions

### Generalization Framework

The project is designed to be extended to the remaining websites with missing persons data:

1. **Modular Architecture**

   - Abstract base classes for scrapers
   - Standardized data models
   - Pluggable site-specific implementations
2. **Planned Extensions**

   - Other state-level Fiscalías
   - National missing persons databases
   - International missing persons resources

### Data Integration

- Standardized output format for cross-database compatibility
- API endpoints for data access
- Integration with existing missing persons databases

## Development Guidelines

1. **Local Development**

   ```bash
   # Start the environment
   docker-compose up -d
   ```
2. **VSCode Integration**

   - Install Remote Development extension
   - Use Command Palette: "Remote-Containers: Attach to Running Container"
   - Select the scraper container
   - All extensions and settings will be preserved
3. **Debugging**

   - VNC access available at `localhost:7900`
   - Selenium grid console at `localhost:4444`
   - Jupyter Lab logs in container output

## Environment Variables

```
SELENIUM_HOST=selenium-hub
SELENIUM_PORT=4444
TZ=America/Mexico_City
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests if applicable
5. Submit a pull request

## Legal Notice

This scraper is intended for research and transparency purposes. Please ensure compliance with local laws and website terms of service when using or modifying this tool.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.
