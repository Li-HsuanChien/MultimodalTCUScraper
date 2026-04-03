Here’s a simple **README.md** based on the steps you want:

---

# MultimodalTCUScraper

This repository contains scripts for scraping and processing Multimodal TCU-related data on Penn State ICDS Roar.

## Setup & Run Instructions (Penn State ICDS Roar)

### 1. Log into ICDS Roar (Penn State RC Terminal)

Open your terminal and connect to Roar:

```bash
ssh <your_psu_id>@submit-portal.hpc.psu.edu
```

(Use the correct Roar login node if your lab provides a different one.)

---

### 2. Clone the Repository

```bash
git clone https://github.com/Li-HsuanChien/MultimodalTCUScraper.git
```

---

### 3. Enter the Project Folder

```bash
cd MultimodalTCUScraper
```

---

### 4. Load Python Module

```bash
module load python/3.13.0
```

---

### 5. Run the Script

```bash
bash run.sh
```

---

## Notes

* Make sure you have access to the required Roar environment permissions.
* If `run.sh` is not executable, you can enable permissions with:

```bash
chmod +x run.sh
```

---
