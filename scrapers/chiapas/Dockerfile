# Use miniconda as base
FROM condaforge/miniforge3

# Set noninteractive installation and timezone
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Mexico_City

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-spa \
    libgl1-mesa-glx \
    libglib2.0-0 \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && dpkg-reconfigure -f noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy environment file
COPY environment.yml .

# Create conda environment from yml
RUN conda env create -f environment.yml && \
    conda clean -afy && \
    conda run -n scraper_docker pip install notebook jupyterlab

# Add conda environment to PATH
ENV PATH /opt/conda/envs/scraper_docker/bin:$PATH

# Expose port 8888 for Jupyter Notebook
EXPOSE 8888

# Create a non-root user
RUN useradd -m -u 1000 scraper
RUN chown -R scraper:scraper /app
USER scraper

# Make sure we're using the right environment
SHELL ["conda", "run", "-n", "scraper_docker", "/bin/bash", "-c"]

# Activate conda environment and run Jupyter
CMD ["conda", "run", "-n", "scraper_docker", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''", "--allow-root", "--NotebookApp.allow_origin='*'"]