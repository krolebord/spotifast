//! Who this build is to the operating system. Spotifast Soloist installs and
//! runs beside upstream Spotifast, so every name a system keys an app by
//! (folders, saved sign-in, single-instance slot, desktop entry, bundle,
//! media controls, login item) differs from upstream's. In-app text still
//! says Spotifast, so the translations keep matching.

/// The name people see: window title, menus, tray, mixer, installers.
pub const NAME: &str = "Spotifast Soloist";
/// The lowercase name: command, desktop entry and icon, folders, slots.
pub const SLUG: &str = "spotifast-soloist";
/// The reverse-domain identity: macOS bundle and the credential store.
pub const BUNDLE_ID: &str = "io.github.krolebord.SpotifastSoloist";
