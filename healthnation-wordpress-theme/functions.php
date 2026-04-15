<?php
/**
 * HealthNation Theme Functions
 * Registers assets, theme support, custom post meta,
 * REST API extensions, and automation helpers.
 */

if ( ! defined( 'ABSPATH' ) ) exit;

define( 'HN_VERSION', '1.0.0' );

/* ─────────────────────────────────────────────────────────────
   CALCULATOR SHORTCODES
   Usage in page content: [hn_calculator type="bmr"]
   Types: bmr, protein, sleep, vo2, hydration, macros
──────────────────────────────────────────────────────────────── */
function hn_calculator_shortcode( $atts ) {
    $atts = shortcode_atts( [ 'type' => 'bmr' ], $atts );
    $type = sanitize_key( $atts['type'] );

    $css = '<style>
.calc-wrap{max-width:760px;margin:0 auto;padding:20px 0}
.calc-intro{font-size:17px;color:#374151;margin-bottom:32px;line-height:1.7}
.calc-box{background:#F6FAF7;border:1px solid #d1e8d8;border-radius:12px;padding:32px;margin-bottom:40px}
.calc-fields{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-bottom:24px}
.calc-field label{display:block;font-weight:600;font-size:14px;color:#111827;margin-bottom:6px}
.calc-field input,.calc-field select{width:100%;padding:10px 14px;border:1px solid #d1d5db;border-radius:8px;font-size:15px;background:white;font-family:inherit}
.calc-field small{display:block;font-size:12px;color:#6B7280;margin-top:4px}
.calc-btn{background:#4A7C59;color:white;border:none;padding:14px 32px;border-radius:50px;font-size:15px;font-weight:600;cursor:pointer;transition:background .15s}
.calc-btn:hover{background:#3d6649}
.calc-result{margin-top:28px;padding:24px;background:white;border-radius:10px;border:1px solid #d1e8d8}
.calc-result h3{font-size:20px;margin-bottom:12px;color:#111827}
.calc-result table{width:100%;border-collapse:collapse;margin:16px 0}
.calc-result th{background:#4A7C59;color:white;padding:10px 14px;text-align:left;font-size:13px}
.calc-result td{padding:10px 14px;border-bottom:1px solid #e5e7eb;font-size:14px}
.calc-note{font-size:12px;color:#6B7280;font-style:italic;margin-top:12px}
</style>';

    $html = '';
    switch ( $type ) {
        case 'bmr':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Sex</label><select id="bmr-sex"><option value="male">Male</option><option value="female">Female</option></select></div>
<div class="calc-field"><label>Age (years)</label><input type="number" id="bmr-age" value="30" min="15" max="100"></div>
<div class="calc-field"><label>Weight (kg)</label><input type="number" id="bmr-weight" value="70" min="30" max="300"></div>
<div class="calc-field"><label>Height (cm)</label><input type="number" id="bmr-height" value="175" min="100" max="250"></div>
</div><button class="calc-btn" onclick="calcBMR()">Calculate BMR</button>
<div class="calc-result" id="bmr-result" style="display:none"></div></div>';
            break;
        case 'protein':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Body Weight (kg)</label><input type="number" id="p-weight" value="70" min="30" max="300"></div>
<div class="calc-field"><label>Goal</label><select id="p-goal"><option value="maintain">Maintain weight</option><option value="muscle">Build muscle</option><option value="lose">Lose fat (preserve muscle)</option><option value="athlete">Endurance athlete</option></select></div>
</div><button class="calc-btn" onclick="calcProtein()">Calculate Protein</button>
<div class="calc-result" id="protein-result" style="display:none"></div></div>';
            break;
        case 'sleep':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Age group</label><select id="s-age"><option value="9">14–17 years (need: 8–10h)</option><option value="8" selected>18–64 years (need: 7–9h)</option><option value="7.5">65+ years (need: 7–8h)</option></select></div>
<div class="calc-field"><label>Avg sleep per night (hours)</label><input type="number" id="s-actual" value="6.5" min="2" max="12" step="0.5"></div>
<div class="calc-field"><label>Days tracked</label><input type="number" id="s-days" value="7" min="1" max="30"></div>
</div><button class="calc-btn" onclick="calcSleep()">Calculate Sleep Debt</button>
<div class="calc-result" id="sleep-result" style="display:none"></div></div>';
            break;
        case 'vo2':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Age (years)</label><input type="number" id="v-age" value="35" min="15" max="80"></div>
<div class="calc-field"><label>Resting Heart Rate (bpm)</label><input type="number" id="v-rhr" value="65" min="35" max="100"><small>Measure first thing in the morning before getting up</small></div>
</div><button class="calc-btn" onclick="calcVO2()">Estimate VO2 Max</button>
<div class="calc-result" id="vo2-result" style="display:none"></div></div>';
            break;
        case 'hydration':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Body Weight (kg)</label><input type="number" id="h-weight" value="70" min="30" max="300"></div>
<div class="calc-field"><label>Activity Level</label><select id="h-activity"><option value="1.0">Sedentary</option><option value="1.2">Lightly active</option><option value="1.4" selected>Moderately active</option><option value="1.6">Very active</option><option value="1.8">Athlete</option></select></div>
<div class="calc-field"><label>Climate</label><select id="h-climate"><option value="1.0">Cool / Temperate</option><option value="1.1">Warm</option><option value="1.2">Hot / Humid</option></select></div>
</div><button class="calc-btn" onclick="calcHydration()">Calculate Water Needs</button>
<div class="calc-result" id="hydration-result" style="display:none"></div></div>';
            break;
        case 'macros':
            $html = '<div class="calc-box"><div class="calc-fields">
<div class="calc-field"><label>Daily Calories (TDEE)</label><input type="number" id="m-cal" value="2200" min="1000" max="5000"><small>Use our <a href="/tools/bmr-calculator/">BMR Calculator</a> first</small></div>
<div class="calc-field"><label>Body Weight (kg)</label><input type="number" id="m-weight" value="70" min="30" max="300"></div>
<div class="calc-field"><label>Goal</label><select id="m-goal"><option value="maintain">Maintain weight</option><option value="muscle">Build muscle</option><option value="lose">Lose fat</option><option value="keto">Ketogenic</option></select></div>
</div><button class="calc-btn" onclick="calcMacros()">Calculate Macros</button>
<div class="calc-result" id="macros-result" style="display:none"></div></div>';
            break;
    }

    return $css . $html;
}
add_shortcode( 'hn_calculator', 'hn_calculator_shortcode' );

/* ─────────────────────────────────────────────────────────────
   NAV WALKER — must be defined before header.php uses it
──────────────────────────────────────────────────────────────── */
if ( ! class_exists( 'HN_Nav_Walker' ) ) :
class HN_Nav_Walker extends Walker_Nav_Menu {
    public function start_el( &$output, $item, $depth = 0, $args = null, $id = 0 ) {
        $url     = $item->url;
        $title   = apply_filters( 'the_title', $item->title, $item->ID );
        $current = in_array( 'current-menu-item', $item->classes ) ? ' aria-current="page"' : '';
        $output .= '<a href="' . esc_url( $url ) . '" class="nav-item"' . $current . '>' . esc_html( $title ) . '</a>';
    }
    public function end_el( &$output, $item, $depth = 0, $args = null ) {}
    public function start_lvl( &$output, $depth = 0, $args = null ) {}
    public function end_lvl( &$output, $depth = 0, $args = null ) {}
}
endif;
define( 'HN_DIR',     get_template_directory() );
define( 'HN_URI',     get_template_directory_uri() );

/* ─────────────────────────────────────────────────────────────
   1. THEME SETUP
──────────────────────────────────────────────────────────────── */
function hn_setup() {
    load_theme_textdomain( 'healthnation', HN_DIR . '/languages' );

    add_theme_support( 'title-tag' );
    add_theme_support( 'post-thumbnails' );
    add_theme_support( 'html5', [ 'search-form', 'comment-form', 'gallery', 'caption', 'script', 'style' ] );
    add_theme_support( 'custom-logo' );
    add_theme_support( 'automatic-feed-links' );
    add_theme_support( 'responsive-embeds' );
    add_theme_support( 'align-wide' );

    // Custom image sizes
    add_image_size( 'hn-card',   600, 400, true );
    add_image_size( 'hn-hero',   1200, 630, true );
    add_image_size( 'hn-thumb',  400, 300, true );

    // Navigation menus
    register_nav_menus( [
        'primary'  => __( 'Primary Navigation', 'healthnation' ),
        'footer'   => __( 'Footer Navigation',   'healthnation' ),
    ] );
}
add_action( 'after_setup_theme', 'hn_setup' );

/* ─────────────────────────────────────────────────────────────
   2. ENQUEUE ASSETS
──────────────────────────────────────────────────────────────── */
function hn_enqueue_assets() {
    // Google Fonts
    wp_enqueue_style(
        'hn-fonts',
        'https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=Inter:wght@300;400;500;600;700&display=swap',
        [],
        null
    );

    // Main stylesheet
    wp_enqueue_style( 'hn-style', get_stylesheet_uri(), [ 'hn-fonts' ], HN_VERSION );

    // Main JS
    wp_enqueue_script( 'hn-main', HN_URI . '/assets/js/main.js', [], HN_VERSION, true );

    // Pass data to JS
    wp_localize_script( 'hn-main', 'hnData', [
        'ajaxUrl' => admin_url( 'admin-ajax.php' ),
        'nonce'   => wp_create_nonce( 'hn_nonce' ),
        'siteUrl' => get_site_url(),
    ] );

    // Comment reply script on single posts
    if ( is_singular() && comments_open() ) {
        wp_enqueue_script( 'comment-reply' );
    }
}
add_action( 'wp_enqueue_scripts', 'hn_enqueue_assets' );

/* ─────────────────────────────────────────────────────────────
   3. CUSTOM POST META (for automation-generated articles)
──────────────────────────────────────────────────────────────── */
function hn_register_post_meta() {
    $meta_fields = [
        'hn_reviewer_name'        => 'string',
        'hn_reviewer_credentials' => 'string',
        'hn_reviewer_specialty'   => 'string',
        'hn_read_time'            => 'integer',
        'hn_citation_count'       => 'integer',
        'hn_key_takeaways'        => 'string',   // JSON array of strings
        'hn_references'           => 'string',   // JSON array
        'hn_focus_keyword'        => 'string',
        'hn_meta_description'     => 'string',
        'hn_last_reviewed'        => 'string',
        'hn_hero_image_url'       => 'string',   // Unsplash URL stored for reference
        'hn_ai_generated'         => 'boolean',  // Flag for automation-generated posts
        'hn_unsplash_photo_id'    => 'string',
        'hn_unsplash_photographer'=> 'string',
    ];

    foreach ( $meta_fields as $key => $type ) {
        register_post_meta( 'post', $key, [
            'show_in_rest'  => true,
            'single'        => true,
            'type'          => $type,
            'auth_callback' => function() { return current_user_can( 'edit_posts' ); },
        ] );
    }
}
add_action( 'init', 'hn_register_post_meta' );

/* ─────────────────────────────────────────────────────────────
   4. REST API — Application Password support + CORS for automation
──────────────────────────────────────────────────────────────── */
// Allow REST API write access via Application Passwords (WordPress 5.6+)
// No code needed — just ensure "Application Passwords" are enabled in Settings > General

// Add CORS headers so your automation script can call from any server
function hn_rest_cors_headers() {
    header( 'Access-Control-Allow-Origin: *' );
    header( 'Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS' );
    header( 'Access-Control-Allow-Headers: Authorization, Content-Type, X-WP-Nonce' );
}
add_action( 'rest_api_init', 'hn_rest_cors_headers' );

// Expose additional fields in REST API responses
function hn_rest_extra_fields( $response, $post, $request ) {
    $response->data['hn_reviewer_name']        = get_post_meta( $post->ID, 'hn_reviewer_name', true );
    $response->data['hn_reviewer_credentials'] = get_post_meta( $post->ID, 'hn_reviewer_credentials', true );
    $response->data['hn_read_time']            = (int) get_post_meta( $post->ID, 'hn_read_time', true );
    $response->data['hn_key_takeaways']        = json_decode( get_post_meta( $post->ID, 'hn_key_takeaways', true ), true );
    $response->data['hn_focus_keyword']        = get_post_meta( $post->ID, 'hn_focus_keyword', true );
    $response->data['hn_meta_description']     = get_post_meta( $post->ID, 'hn_meta_description', true );
    return $response;
}
add_filter( 'rest_prepare_post', 'hn_rest_extra_fields', 10, 3 );

/* ─────────────────────────────────────────────────────────────
   5. SEO — Meta tags output (works without Rank Math too)
──────────────────────────────────────────────────────────────── */
function hn_output_seo_meta() {
    // Skip if Rank Math or Yoast is active (they handle this)
    if ( function_exists( 'rank_math' ) || defined( 'WPSEO_VERSION' ) ) return;

    if ( is_singular() ) {
        global $post;
        $meta_desc    = get_post_meta( $post->ID, 'hn_meta_description', true );
        $focus_kw     = get_post_meta( $post->ID, 'hn_focus_keyword', true );
        $thumb        = get_the_post_thumbnail_url( $post->ID, 'hn-hero' );

        if ( $meta_desc ) {
            echo '<meta name="description" content="' . esc_attr( $meta_desc ) . '">' . "\n";
        }

        // Open Graph
        echo '<meta property="og:type" content="article">' . "\n";
        echo '<meta property="og:title" content="' . esc_attr( get_the_title() ) . '">' . "\n";
        echo '<meta property="og:url" content="' . esc_url( get_permalink() ) . '">' . "\n";
        if ( $meta_desc ) {
            echo '<meta property="og:description" content="' . esc_attr( $meta_desc ) . '">' . "\n";
        }
        if ( $thumb ) {
            echo '<meta property="og:image" content="' . esc_url( $thumb ) . '">' . "\n";
        }

        // Twitter Card
        echo '<meta name="twitter:card" content="summary_large_image">' . "\n";
    }
}
add_action( 'wp_head', 'hn_output_seo_meta', 5 );

/* ─────────────────────────────────────────────────────────────
   6. STRUCTURED DATA (JSON-LD) — Article + FAQPage schema
──────────────────────────────────────────────────────────────── */
function hn_structured_data() {
    if ( ! is_singular( 'post' ) ) return;

    global $post;

    $reviewer    = get_post_meta( $post->ID, 'hn_reviewer_name', true );
    $credentials = get_post_meta( $post->ID, 'hn_reviewer_credentials', true );
    $thumb       = get_the_post_thumbnail_url( $post->ID, 'hn-hero' );
    $modified    = get_the_modified_date( 'c', $post->ID );
    $published   = get_the_date( 'c', $post->ID );

    $article_schema = [
        '@context'         => 'https://schema.org',
        '@type'            => 'MedicalWebPage',
        'headline'         => get_the_title(),
        'datePublished'    => $published,
        'dateModified'     => $modified,
        'url'              => get_permalink(),
        'author'           => [
            '@type' => 'Organization',
            'name'  => 'HealthNation Editorial Team',
            'url'   => get_site_url(),
        ],
        'publisher' => [
            '@type' => 'Organization',
            'name'  => 'HealthNation',
            'url'   => get_site_url(),
            'logo'  => [ '@type' => 'ImageObject', 'url' => get_site_url() . '/wp-content/themes/healthnation/assets/img/logo.png' ],
        ],
        'description' => get_post_meta( $post->ID, 'hn_meta_description', true ) ?: get_the_excerpt(),
        'medicalAudience' => [ '@type' => 'MedicalAudience', 'audienceType' => 'Patient' ],
    ];

    if ( $reviewer ) {
        $article_schema['reviewedBy'] = [
            '@type'       => 'Physician',
            'name'        => $reviewer,
            'description' => $credentials,
        ];
    }

    if ( $thumb ) {
        $article_schema['image'] = $thumb;
    }

    echo '<script type="application/ld+json">' . wp_json_encode( $article_schema, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE ) . '</script>' . "\n";
}
add_action( 'wp_head', 'hn_structured_data' );

/* ─────────────────────────────────────────────────────────────
   7. BREADCRUMBS
──────────────────────────────────────────────────────────────── */
function hn_breadcrumbs() {
    if ( is_front_page() ) return;

    echo '<nav class="breadcrumbs" aria-label="Breadcrumb"><div class="container"><ol itemscope itemtype="https://schema.org/BreadcrumbList">';
    echo '<li itemprop="itemListElement" itemscope itemtype="https://schema.org/ListItem">';
    echo '<a itemprop="item" href="' . home_url() . '"><span itemprop="name">Home</span></a>';
    echo '<meta itemprop="position" content="1">';
    echo '</li>';

    $position = 2;

    if ( is_category() ) {
        echo '<li itemprop="itemListElement" itemscope itemtype="https://schema.org/ListItem">';
        echo '<span itemprop="name">' . single_cat_title( '', false ) . '</span>';
        echo '<meta itemprop="position" content="' . $position . '">';
        echo '</li>';
    } elseif ( is_singular( 'post' ) ) {
        $categories = get_the_category();
        if ( $categories ) {
            $cat = $categories[0];
            echo '<li itemprop="itemListElement" itemscope itemtype="https://schema.org/ListItem">';
            echo '<a itemprop="item" href="' . get_category_link( $cat->term_id ) . '"><span itemprop="name">' . esc_html( $cat->name ) . '</span></a>';
            echo '<meta itemprop="position" content="' . $position . '">';
            echo '</li>';
            $position++;
        }
        echo '<li itemprop="itemListElement" itemscope itemtype="https://schema.org/ListItem">';
        echo '<span itemprop="name">' . get_the_title() . '</span>';
        echo '<meta itemprop="position" content="' . $position . '">';
        echo '</li>';
    }

    echo '</ol></div></nav>';
}

/* ─────────────────────────────────────────────────────────────
   8. HELPER FUNCTIONS
──────────────────────────────────────────────────────────────── */

// Estimated reading time
function hn_reading_time( $post_id = null ) {
    $stored = get_post_meta( $post_id ?: get_the_ID(), 'hn_read_time', true );
    if ( $stored ) return (int) $stored;

    $content   = get_post_field( 'post_content', $post_id ?: get_the_ID() );
    $word_count = str_word_count( wp_strip_all_tags( $content ) );
    return max( 1, (int) ceil( $word_count / 225 ) );
}

// Key takeaways list
function hn_key_takeaways( $post_id = null ) {
    $raw = get_post_meta( $post_id ?: get_the_ID(), 'hn_key_takeaways', true );
    if ( ! $raw ) return [];
    $decoded = json_decode( $raw, true );
    return is_array( $decoded ) ? $decoded : [];
}

// Reviewer block HTML
function hn_reviewer_block( $post_id = null ) {
    $id   = $post_id ?: get_the_ID();
    $name = get_post_meta( $id, 'hn_reviewer_name', true );
    if ( ! $name ) return '';

    $creds     = get_post_meta( $id, 'hn_reviewer_credentials', true );
    $specialty = get_post_meta( $id, 'hn_reviewer_specialty', true );
    $reviewed  = get_post_meta( $id, 'hn_last_reviewed', true );
    $date_str  = $reviewed ?: get_the_modified_date( 'F Y', $id );

    ob_start();
    ?>
    <div class="reviewer-block">
        <div class="reviewer-block-icon">🩺</div>
        <div class="reviewer-block-text">
            <strong>Reviewed by: <?php echo esc_html( $name ); ?><?php if ( $creds ) echo ', ' . esc_html( $creds ); ?></strong>
            <span><?php if ( $specialty ) echo esc_html( $specialty ) . ' · '; ?>Last reviewed: <?php echo esc_html( $date_str ); ?></span>
        </div>
    </div>
    <?php
    return ob_get_clean();
}

// Article card HTML (reusable)
function hn_article_card( $post_id ) {
    $post     = get_post( $post_id );
    $thumb    = get_the_post_thumbnail_url( $post_id, 'hn-card' );
    $cat      = get_the_category( $post_id );
    $cat_name = $cat ? $cat[0]->name : '';
    $cat_url  = $cat ? get_category_link( $cat[0]->term_id ) : '#';
    $read     = hn_reading_time( $post_id );

    ob_start();
    ?>
    <article class="article-card">
        <?php if ( $thumb ) : ?>
        <a href="<?php echo get_permalink( $post_id ); ?>" class="article-card-img">
            <img src="<?php echo esc_url( $thumb ); ?>" alt="<?php echo esc_attr( get_the_title( $post_id ) ); ?>" loading="lazy" />
        </a>
        <?php endif; ?>
        <div class="article-card-body">
            <?php if ( $cat_name ) : ?>
            <a href="<?php echo esc_url( $cat_url ); ?>" class="article-cat-label"><?php echo esc_html( $cat_name ); ?></a>
            <?php endif; ?>
            <h3><a href="<?php echo get_permalink( $post_id ); ?>"><?php echo get_the_title( $post_id ); ?></a></h3>
        </div>
        <div class="article-card-footer">
            <span class="article-read-time"><?php echo $read; ?> min read</span>
            <?php if ( $cat_name ) : ?>
            <span class="article-tag-pill"><?php echo esc_html( $cat_name ); ?></span>
            <?php endif; ?>
        </div>
    </article>
    <?php
    return ob_get_clean();
}

/* ─────────────────────────────────────────────────────────────
   9. AJAX — Newsletter signup handler
──────────────────────────────────────────────────────────────── */
function hn_newsletter_signup() {
    check_ajax_referer( 'hn_nonce', 'nonce' );
    $email = sanitize_email( $_POST['email'] ?? '' );
    if ( ! is_email( $email ) ) {
        wp_send_json_error( [ 'message' => 'Invalid email address.' ] );
    }
    // TODO: integrate with Mailchimp / ConvertKit API
    // For now, store in options as a simple list
    $subscribers = get_option( 'hn_newsletter_subscribers', [] );
    if ( ! in_array( $email, $subscribers ) ) {
        $subscribers[] = $email;
        update_option( 'hn_newsletter_subscribers', $subscribers );
    }
    wp_send_json_success( [ 'message' => 'Thank you! Check your inbox.' ] );
}
add_action( 'wp_ajax_hn_newsletter',        'hn_newsletter_signup' );
add_action( 'wp_ajax_nopriv_hn_newsletter', 'hn_newsletter_signup' );

/* ─────────────────────────────────────────────────────────────
   10. PERFORMANCE
──────────────────────────────────────────────────────────────── */
// Remove unnecessary head bloat
remove_action( 'wp_head', 'wp_generator' );
remove_action( 'wp_head', 'wlwmanifest_link' );
remove_action( 'wp_head', 'rsd_link' );
remove_action( 'wp_head', 'wp_shortlink_wp_head' );
remove_action( 'wp_head', 'adjacent_posts_rel_link_wp_head' );

// Add preconnect for Google Fonts performance
function hn_preconnect_fonts() {
    echo '<link rel="preconnect" href="https://fonts.googleapis.com">' . "\n";
    echo '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>' . "\n";
}
add_action( 'wp_head', 'hn_preconnect_fonts', 1 );

/* ─────────────────────────────────────────────────────────────
   11. CATEGORY DESCRIPTIONS & IMAGES
──────────────────────────────────────────────────────────────── */
function hn_register_category_image() {
    add_action( 'created_category', 'hn_save_category_image' );
    add_action( 'edited_category',  'hn_save_category_image' );
    add_action( 'category_add_form_fields',  'hn_category_image_field' );
    add_action( 'category_edit_form_fields', 'hn_category_image_field' );
}
add_action( 'init', 'hn_register_category_image' );

function hn_category_image_field( $term ) {
    $image_id = is_object( $term ) ? get_term_meta( $term->term_id, 'category_image_id', true ) : '';
    ?>
    <div class="form-field">
        <label>Category Image</label>
        <input type="hidden" name="category_image_id" value="<?php echo esc_attr( $image_id ); ?>" id="category_image_id" />
        <button type="button" class="button" onclick="hn_select_image()">Select Image</button>
        <?php if ( $image_id ) echo wp_get_attachment_image( $image_id, 'thumbnail' ); ?>
    </div>
    <?php
}

function hn_save_category_image( $term_id ) {
    if ( isset( $_POST['category_image_id'] ) ) {
        update_term_meta( $term_id, 'category_image_id', (int) $_POST['category_image_id'] );
    }
}

function hn_get_category_image_url( $term_id, $size = 'hn-card' ) {
    $image_id = get_term_meta( $term_id, 'category_image_id', true );
    if ( $image_id ) return wp_get_attachment_image_url( $image_id, $size );
    // Fallback Unsplash images per category slug
    $category = get_term( $term_id, 'category' );
    $fallbacks = [
        'nutrition'     => 'https://images.unsplash.com/photo-1512621776951-a57141f2eefd?w=600&q=75&fit=crop',
        'fitness'       => 'https://images.unsplash.com/photo-1517836357463-d25dfeac3438?w=600&q=75&fit=crop',
        'mental-health' => 'https://images.unsplash.com/photo-1545205597-3d9d02c29597?w=600&q=75&fit=crop',
        'longevity'     => 'https://images.unsplash.com/photo-1571019613454-1cb2f99b2d8b?w=600&q=75&fit=crop',
        'sleep'         => 'https://images.unsplash.com/photo-1541781774459-bb2af2f05b55?w=600&q=75&fit=crop',
        'conditions'    => 'https://images.unsplash.com/photo-1576091160399-112ba8d25d1d?w=600&q=75&fit=crop',
    ];
    return $fallbacks[ $category->slug ] ?? 'https://images.unsplash.com/photo-1490645935967-10de6ba17061?w=600&q=75&fit=crop';
}
