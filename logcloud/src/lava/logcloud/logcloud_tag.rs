use std::fs::{self, File};
use std::io;
use std::path::Path;

pub(crate) fn move_tag_or_create_empty(source: &Path, target: &Path) -> io::Result<()> {
    if source.exists() {
        fs::rename(source, target)
    } else {
        File::create(target).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::move_tag_or_create_empty;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn missing_tag_for_all_outlier_chunk_creates_empty_target() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir = std::env::temp_dir().join(format!(
            "rottnest-logcloud-empty-tag-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&test_dir).unwrap();

        let source = test_dir.join("variable_0_tag.txt");
        let target = test_dir.join("compressed").join("variable_0_tag.txt");
        fs::create_dir_all(target.parent().unwrap()).unwrap();

        move_tag_or_create_empty(&source, &target).unwrap();

        assert!(target.is_file());
        assert_eq!(fs::metadata(&target).unwrap().len(), 0);
        assert!(!source.exists());

        fs::remove_dir_all(test_dir).unwrap();
    }
}
