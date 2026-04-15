<!DOCTYPE html>
<html <?php language_attributes(); ?>>
<head>
  <meta charset="<?php bloginfo( 'charset' ); ?>" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <?php wp_head(); ?>
</head>
<body <?php body_class(); ?>>
<?php wp_body_open(); ?>

<header class="site-header" id="site-header">
  <div class="header-inner">

    <a href="<?php echo home_url( '/' ); ?>" class="site-logo" rel="home">
      <div class="logo-mark" aria-hidden="true">🌿</div>
      <?php bloginfo( 'name' ); ?>
    </a>

    <nav class="main-nav" aria-label="Primary navigation">
      <?php
      wp_nav_menu( [
        'theme_location' => 'primary',
        'container'      => false,
        'menu_class'     => '',
        'fallback_cb'    => 'hn_fallback_nav',
        'items_wrap'     => '%3$s',
        'depth'          => 1,
        'walker'         => new HN_Nav_Walker(),
      ] );
      ?>
    </nav>

    <button class="nav-search-btn" aria-label="Search" onclick="document.getElementById('search-modal').classList.toggle('active')">
      <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="11" cy="11" r="8"/><path d="M21 21l-4.35-4.35"/>
      </svg>
    </button>

    <a href="#newsletter" class="nav-cta">Weekly Insights</a>

    <button class="menu-toggle" aria-label="Open menu" aria-expanded="false" id="menu-toggle">
      <span></span><span></span><span></span>
    </button>

  </div>
</header>

<!-- Mobile nav drawer -->
<div class="mobile-nav-overlay" id="mobile-nav-overlay" aria-hidden="true">
  <div class="mobile-nav-drawer">
    <button class="mobile-nav-close" id="mobile-nav-close" aria-label="Close menu">✕</button>
    <nav>
      <?php
      wp_nav_menu( [
        'theme_location' => 'primary',
        'container'      => false,
        'menu_class'     => 'mobile-menu-list',
        'fallback_cb'    => 'hn_fallback_nav',
        'depth'          => 2,
      ] );
      ?>
      <a href="#newsletter" class="nav-cta" style="display:inline-block;margin-top:20px">Weekly Insights</a>
    </nav>
  </div>
</div>

<!-- Search modal -->
<div class="search-modal" id="search-modal" role="dialog" aria-modal="true" aria-label="Search">
  <div class="search-modal-inner">
    <form role="search" method="get" action="<?php echo home_url( '/' ); ?>">
      <input type="search" name="s" placeholder="Search health topics, conditions, nutrition…"
             value="<?php echo get_search_query(); ?>" autofocus />
      <button type="submit">Search</button>
    </form>
    <button class="search-modal-close" onclick="document.getElementById('search-modal').classList.remove('active')" aria-label="Close search">✕</button>
  </div>
</div>

<?php
// Fallback nav when no menu is assigned
function hn_fallback_nav() {
    $cats = get_categories( [ 'number' => 6, 'hide_empty' => false ] );
    foreach ( $cats as $cat ) {
        echo '<a href="' . esc_url( get_category_link( $cat->term_id ) ) . '">' . esc_html( $cat->name ) . '</a>';
    }
}
?>
