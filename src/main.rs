use clap::Parser;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "guzzler")]
#[command(about = "Memory consumption tool")]
struct Args {
    /// Time interval for RAM consumption (default: 100ms)
    #[arg(long, default_value = "100ms")]
    tick_ram_eat: String,

    /// Amount of RAM to consume (default: 10MB)
    #[arg(long, default_value = "10MB")]
    byte_ram: String,

    /// Number of memory allocation threads (default: 4)
    #[arg(long, default_value = "4")]
    memory_thread_count: usize,

    /// Path to file for read/write operations in separate thread
    #[arg(long)]
    read_write_filename: Option<String>,

    /// Size of the read/write file (default: 1GB)
    #[arg(long, default_value = "1GB")]
    read_write_filename_size: String,

    /// Time interval for disk read operations (default: 10ms)
    #[arg(long, default_value = "10ms")]
    tick_disk_read: String,

    /// Size of each disk read operation (default: 10MB)
    #[arg(long, default_value = "10MB")]
    tick_disk_size: String,

    /// Initialize/overwrite the read-write file (default: false)
    #[arg(long, default_value = "false")]
    init_read_write_filename: bool,

    /// Time interval for info loop output (default: 1s)
    #[arg(long, default_value = "1s")]
    info_loop_tick_duration: String,
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    if s.ends_with("ms") {
        let ms: u64 = s.trim_end_matches("ms").parse().map_err(|_| "Invalid duration format")?;
        Ok(Duration::from_millis(ms))
    } else if s.ends_with("s") {
        let secs: u64 = s.trim_end_matches("s").parse().map_err(|_| "Invalid duration format")?;
        Ok(Duration::from_secs(secs))
    } else {
        Err("Duration must end with 'ms' or 's'".to_string())
    }
}

fn parse_bytes(s: &str) -> Result<usize, String> {
    if s.ends_with("GB") {
        let gb: usize = s.trim_end_matches("GB").parse().map_err(|_| "Invalid byte format")?;
        Ok(gb * 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        let mb: usize = s.trim_end_matches("MB").parse().map_err(|_| "Invalid byte format")?;
        Ok(mb * 1024 * 1024)
    } else if s.ends_with("KB") {
        let kb: usize = s.trim_end_matches("KB").parse().map_err(|_| "Invalid byte format")?;
        Ok(kb * 1024)
    } else if s.ends_with("B") {
        let bytes: usize = s.trim_end_matches("B").parse().map_err(|_| "Invalid byte format")?;
        Ok(bytes)
    } else {
        s.parse::<usize>().map_err(|_| "Invalid byte format".to_string())
    }
}

fn format_speed(bytes_per_sec: f64) -> String {
    if bytes_per_sec >= 1024.0 * 1024.0 {
        format!("{:>6.1} MB/s", bytes_per_sec / (1024.0 * 1024.0))
    } else if bytes_per_sec >= 1024.0 {
        format!("{:>6.1} KB/s", bytes_per_sec / 1024.0)
    } else {
        format!("{:>6.1} B/s", bytes_per_sec)
    }
}

fn format_size(bytes: f64) -> String {
    if bytes >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:>6.1} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024.0 * 1024.0 {
        format!("{:>6.1} MB", bytes / (1024.0 * 1024.0))
    } else if bytes >= 1024.0 {
        format!("{:>6.1} KB", bytes / 1024.0)
    } else {
        format!("{:>6.1} B", bytes)
    }
}

fn get_cgroup_memory_stats() -> Result<(usize, usize, usize), String> {
    let memory_stat_path = "/sys/fs/cgroup/memory.stat";
    let memory_current_path = "/sys/fs/cgroup/memory.current";

    let content = std::fs::read_to_string(memory_stat_path)
        .map_err(|e| format!("Failed to read {}: {}", memory_stat_path, e))?;

    let mut anon_value = None;
    let mut file_value = None;

    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            match parts[0] {
                "anon" => {
                    anon_value = parts[1].parse::<usize>()
                        .map_err(|e| format!("Failed to parse anon value: {}", e))
                        .ok();
                }
                "file" => {
                    file_value = parts[1].parse::<usize>()
                        .map_err(|e| format!("Failed to parse file value: {}", e))
                        .ok();
                }
                _ => {}
            }
        }
    }

    // Читаем memory.current
    let current_content = std::fs::read_to_string(memory_current_path)
        .map_err(|e| format!("Failed to read {}: {}", memory_current_path, e))?;

    let current_value = current_content.trim().parse::<usize>()
        .map_err(|e| format!("Failed to parse memory.current value: {}", e))?;

    match (anon_value, file_value) {
        (Some(anon), Some(file)) => Ok((anon, file, current_value)),
        _ => Err("Could not find both anon and file values in memory.stat".to_string())
    }
}

fn get_rss_anon() -> Result<usize, String> {
    let pid = std::process::id();
    let status_path = format!("/proc/{}/status", pid);

    let content = std::fs::read_to_string(&status_path)
        .map_err(|e| format!("Failed to read {}: {}", status_path, e))?;

    for line in content.lines() {
        if line.starts_with("RssAnon:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let value = parts[1].parse::<usize>()
                    .map_err(|e| format!("Failed to parse RssAnon value: {}", e))?;
                // RssAnon в /proc/*/status указывается в килобайтах, конвертируем в байты
                return Ok(value * 1024);
            }
        }
    }

    Err("RssAnon field not found in /proc/*/status".to_string())
}

fn main() {
    let args = Args::parse();

    let mem_tick_duration = match parse_duration(&args.tick_ram_eat) {
        Ok(duration) => duration,
        Err(e) => {
            eprintln!("Error parsing tick_ram_eat: {}", e);
            std::process::exit(1);
        }
    };

    let ram_bytes = match parse_bytes(&args.byte_ram) {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Error parsing byte_ram: {}", e);
            std::process::exit(1);
        }
    };

    let file_size = match parse_bytes(&args.read_write_filename_size) {
        Ok(size) => size,
        Err(e) => {
            eprintln!("Error parsing read_write_filename_size: {}", e);
            std::process::exit(1);
        }
    };

    let disk_read_duration = match parse_duration(&args.tick_disk_read) {
        Ok(duration) => duration,
        Err(e) => {
            eprintln!("Error parsing tick_disk_read: {}", e);
            std::process::exit(1);
        }
    };

    let disk_read_size = match parse_bytes(&args.tick_disk_size) {
        Ok(size) => size,
        Err(e) => {
            eprintln!("Error parsing tick_disk_size: {}", e);
            std::process::exit(1);
        }
    };

    println!("Guzzler started with:");
    println!("  Tick interval: {:?}", mem_tick_duration);
    println!("  Baloon to consume: {} bytes ({} MB)", ram_bytes, ram_bytes / 1024 / 1024);
    println!("  Memory threads: {}", args.memory_thread_count);
    if let Some(ref filename) = args.read_write_filename {
        println!("  Read/Write file: {}", filename);
        println!("  File size: {} bytes ({} MB)", file_size, file_size / 1024 / 1024);
        println!("  Disk read interval: {:?}", disk_read_duration);
        println!("  Disk read size: {} bytes ({} MB)", disk_read_size, disk_read_size / 1024 / 1024);
    }
    println!("Starting memory allocation...\n");

    // Инициализация файла заданного размера (если указан)
    if let Some(ref filename) = args.read_write_filename {
        if args.init_read_write_filename {
            println!("Initializing file {} with size {} bytes...", filename, file_size);
            match std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(filename)
            {
                Ok(mut file) => {
                    use std::io::Write;

                    // Заполняем файл нулями блоками по 1MB для эффективности
                    let chunk_size = 1024 * 1024; // 1MB
                    let zero_chunk = vec![0u8; chunk_size];
                    let mut written = 0;

                    while written < file_size {
                        let to_write = std::cmp::min(chunk_size, file_size - written);
                        if let Err(e) = file.write_all(&zero_chunk[..to_write]) {
                            eprintln!("Error writing zeros to file {}: {}", filename, e);
                            break;
                        }
                        written += to_write;
                    }

                    if written == file_size {
                        println!("File {} successfully initialized with {} bytes of zeros", filename, file_size);
                    } else {
                        eprintln!("Warning: Only wrote {} of {} bytes to file {}", written, file_size, filename);
                    }
                }
                Err(e) => {
                    eprintln!("Error creating/opening file {}: {}", filename, e);
                    std::process::exit(1);
                }
            }
        } else {
            println!("File {} already exists, skipping initialization.", filename);
        }
    }

    // Общая статистика между потоками
    let total_allocated = Arc::new(Mutex::new(0usize));
    let allocation_count = Arc::new(Mutex::new(0usize));
    let total_read = Arc::new(Mutex::new(0usize));
    let read_count = Arc::new(Mutex::new(0usize));

    // Клоны для потока чтения диска
    let total_read_clone = Arc::clone(&total_read);
    let read_count_clone = Arc::clone(&read_count);

    // Поток для работы с файлом (если указан)
    let filename_for_write = args.read_write_filename.clone();
    let _file_thread = if let Some(filename) = filename_for_write {
        Some(thread::spawn(move || {
            use std::fs::OpenOptions;
            use std::io::{Write, Read, Seek, SeekFrom};

            loop {
                match OpenOptions::new()
                    .write(true)
                    .read(true)
                    .open(&filename)
                {
                    Ok(mut file) => {
                        // Записываем данные в файл
                        let data = format!("Timestamp: {}\n", std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs());

                        if let Err(e) = file.write_all(data.as_bytes()) {
                            eprintln!("Error writing to file {}: {}", filename, e);
                        }

                        // Читаем файл обратно
                        if let Err(e) = file.seek(SeekFrom::Start(0)) {
                            eprintln!("Error seeking file {}: {}", filename, e);
                        } else {
                            let mut contents = String::new();
                            if let Err(e) = file.read_to_string(&mut contents) {
                                eprintln!("Error reading file {}: {}", filename, e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error opening file {}: {}", filename, e);
                    }
                }

                // Ждем секунду перед следующей операцией
                thread::sleep(Duration::from_secs(1));
            }
        }))
    } else {
        None
    };

    // Потоки для выделения памяти
    let mut allocation_threads = Vec::new();
    for _ in 0..args.memory_thread_count {
        let total_allocated_clone = Arc::clone(&total_allocated);
        let allocation_count_clone = Arc::clone(&allocation_count);
        let mem_tick_duration = mem_tick_duration.clone();
        let ram_bytes = ram_bytes;

        let thread_handle = thread::spawn(move || {
            use rand::RngCore;
            let mut rng = rand::thread_rng();
            let mut allocated_chunks: Vec<Vec<u8>> = Vec::new();

            loop {
                // Выделяем память и заполняем рандомными байтами
                let mut chunk = vec![0u8; ram_bytes];
                rng.fill_bytes(&mut chunk);
                allocated_chunks.push(chunk);

                // Обновляем статистику
                {
                    let mut total = total_allocated_clone.lock().unwrap();
                    *total += ram_bytes;
                }
                {
                    let mut count = allocation_count_clone.lock().unwrap();
                    *count += 1;
                }

                // Ждем следующий тик
                thread::sleep(mem_tick_duration);
            }
        });

        allocation_threads.push(thread_handle);
    }

    // Поток для чтения диска (если указан файл)
    let filename_for_disk_read = args.read_write_filename.clone();
    let _disk_read_thread = if let Some(filename) = filename_for_disk_read {
        Some(thread::spawn(move || {
            use std::fs::File;
            use std::io::{Read, Seek, SeekFrom};

            loop {
                match File::open(&filename) {
                    Ok(mut file) => {
                        // Получаем размер файла
                        if let Ok(metadata) = file.metadata() {
                            let file_len = metadata.len() as usize;
                            let mut buffer = vec![0u8; disk_read_size];
                            let mut total_read_from_file = 0;

                            // Читаем файл до конца
                            while total_read_from_file < file_len {
                                match file.read(&mut buffer) {
                                    Ok(0) => break, // EOF
                                    Ok(bytes_read) => {
                                        total_read_from_file += bytes_read;

                                        // Обновляем статистику чтения
                                        {
                                            let mut total = total_read_clone.lock().unwrap();
                                            *total += bytes_read;
                                        }
                                        {
                                            let mut count = read_count_clone.lock().unwrap();
                                            *count += 1;
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Error reading file {}: {}", filename, e);
                                        break;
                                    }
                                }

                                // Ждем заданный интервал между чтениями
                                thread::sleep(disk_read_duration);
                            }

                            // Если дочитали до конца, перематываем в начало
                            if total_read_from_file >= file_len {
                                if let Err(e) = file.seek(SeekFrom::Start(0)) {
                                    eprintln!("Error seeking to start of file {}: {}", filename, e);
                                }
                            }
                        } else {
                            eprintln!("Error getting metadata for file {}", filename);
                            thread::sleep(Duration::from_secs(1));
                        }
                    }
                    Err(e) => {
                        eprintln!("Error opening file {} for reading: {}", filename, e);
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }))
    } else {
        None
    };

    // Сохраняем флаг наличия файла для использования в статистике
    let has_file = args.read_write_filename.is_some();

    // Главный поток для вывода статистики каждую секунду
    let start_time = Instant::now();
    let mut last_total = 0usize;
    let mut last_read_total = 0usize;

    // Параметр для интервала вывода информации
    let info_loop_tick_duration = match parse_duration(&args.info_loop_tick_duration) {
        Ok(duration) => duration,
        Err(e) => {
            eprintln!("Error parsing info_loop_tick_duration: {}", e);
            std::process::exit(1);
        }
    };

    loop {
        thread::sleep(info_loop_tick_duration);

        let current_total = {
            let total = total_allocated.lock().unwrap();
            *total
        };

        let current_count = {
            let count = allocation_count.lock().unwrap();
            *count
        };

        let current_read_total = {
            let total = total_read.lock().unwrap();
            *total
        };

        let _current_read_count = {
            let count = read_count.lock().unwrap();
            *count
        };

        let elapsed = start_time.elapsed();
        let allocated_this_period = current_total - last_total;
        let read_this_period = current_read_total - last_read_total;

        // Получаем значения anon, file и current из cgroup
        let (cgroup_anon, cgroup_file, cgroup_current) = match get_cgroup_memory_stats() {
            Ok((anon, file, current)) => (anon, file, current),
            Err(_) => (0, 0, 0), // Если не удалось получить, используем 0
        };

        // Получаем RssAnon из /proc/<pid>/status
        let rss_anon = get_rss_anon().unwrap_or_else(|_| 0);

        // Вычисляем скорость в секунду на основе info_loop_tick_duration
        let period_seconds = info_loop_tick_duration.as_secs_f64();
        let alloc_speed_per_sec = allocated_this_period as f64 / period_seconds;
        let read_speed_per_sec = read_this_period as f64 / period_seconds;

        let alloc_speed_formatted = format_speed(alloc_speed_per_sec);
        let read_speed_formatted = format_speed(read_speed_per_sec);
        let total_formatted = format_size(current_total as f64);
        let total_read_formatted = format_size(current_read_total as f64);
        let cgroup_anon_formatted = format_size(cgroup_anon as f64);
        let cgroup_file_formatted = format_size(cgroup_file as f64);
        let cgroup_current_formatted = format_size(cgroup_current as f64);
        let rss_anon_formatted = format_size(rss_anon as f64);

        if has_file {
            println!(
                "Time: {:>6.1}s | Baloon: {} | Allocs: {:>8} | Baloon Speed: {} | Disk Read: {} | Read Speed: {} | Status.Anon: {} | Status.File: {} | Memory.Current: {} | RssAnon: {}",
                elapsed.as_secs_f64(),
                total_formatted,
                current_count,
                alloc_speed_formatted,
                total_read_formatted,
                read_speed_formatted,
                cgroup_anon_formatted,
                cgroup_file_formatted,
                cgroup_current_formatted,
                rss_anon_formatted
            );
        } else {
            println!(
                "Time: {:>6.1}s | Baloon: {} | Allocs: {:>8} | Baloon Speed: {} | Status.Anon: {} | Status.File: {} | Memory.Current: {} | RssAnon: {}",
                elapsed.as_secs_f64(),
                total_formatted,
                current_count,
                alloc_speed_formatted,
                cgroup_anon_formatted,
                cgroup_file_formatted,
                cgroup_current_formatted,
                rss_anon_formatted
            );
        }

        last_total = current_total;
        last_read_total = current_read_total;
    }
}
