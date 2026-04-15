<?php get_header(); ?>
<?php while ( have_posts() ) : the_post(); ?>

<?php
$reviewer     = get_post_meta( get_the_ID(), 'hn_reviewer_name', true );
$rev_creds    = get_post_meta( get_the_ID(), 'hn_reviewer_credentials', true );
$rev_spec     = get_post_meta( get_the_ID(), 'hn_reviewer_specialty', true );
$read_time    = hn_reading_time();
$takeaways    = hn_key_takeaways();
$hero_img     = get_the_post_thumbnail_url( get_the_ID(), 'hn-hero' );
$categories   = get_the_category();
$cat          = $categories ? $categories[0] : null;
$citations    = get_post_meta( get_the_ID(), 'hn_citation_count', true );
$last_reviewed = get_post_meta( get_the_ID(), 'hn_last_reviewed', true );
?>

<!-- Article Header -->
<div class="single-header">
  <div class="container">
    <?php hn_breadcrumbs(); ?>

    <?php if ( $cat ) : ?>
    <a href="<?php echo get_category_link( $cat->term_id ); ?>" class="single-category-link">
      <?php echo esc_html( $cat->name ); ?>
    </a>
    <?php endif; ?>

    <h1 class="single-title"><?php the_title(); ?></h1>

    <div class="single-meta">
      <span>By <strong><?php the_author(); ?></strong></span>
      <span class="meta-divider" aria-hidden="true">·</span>
      <span><?php echo $last_reviewed ? 'Updated ' . esc_html( $last_reviewed ) : 'Published ' . get_the_date( 'F j, Y' ); ?></span>
      <span class="meta-divider" aria-hidden="true">·</span>
      <span><?php echo $read_time; ?> min read</span>
      <?php if ( $citations ) : ?>
      <span class="meta-divider" aria-hidden="true">·</span>
      <span><?php echo $citations; ?> citations</span>
      <?php endif; ?>
    </div>

    <?php echo hn_reviewer_block(); ?>

    <?php if ( ! empty( $takeaways ) ) : ?>
    <div class="key-takeaways">
      <h4>Key Takeaways</h4>
      <ul>
        <?php foreach ( $takeaways as $point ) : ?>
        <li><?php echo esc_html( $point ); ?></li>
        <?php endforeach; ?>
      </ul>
    </div>
    <?php endif; ?>
  </div>
</div>

<!-- Hero Image -->
<?php if ( $hero_img ) : ?>
<div class="single-hero-img container">
  <img src="<?php echo esc_url( $hero_img ); ?>"
       alt="<?php echo esc_attr( get_the_title() ); ?>"
       width="1200" height="630" />
  <?php
  $photo_credit = get_post_meta( get_the_ID(), 'hn_unsplash_photographer', true );
  if ( $photo_credit ) :
  ?>
  <p style="font-size:11px;color:var(--ink-faint);margin-top:6px;text-align:right">
    Photo: <?php echo esc_html( $photo_credit ); ?> / Unsplash
  </p>
  <?php endif; ?>
</div>
<?php endif; ?>

<!-- Article Content + Sidebar -->
<div class="content-with-sidebar">
  <main class="entry-content" id="main-content">
    <?php the_content(); ?>

    <!-- Medical Disclaimer -->
    <div class="medical-disclaimer" role="note">
      <strong>Medical Disclaimer:</strong> This article is for informational purposes only and does not constitute medical advice, diagnosis, or treatment. Always consult a qualified healthcare provider before making changes to your diet, exercise routine, supplement regimen, or any other health-related decisions. Individual results may vary.
    </div>

    <!-- References -->
    <?php
    $refs_raw = get_post_meta( get_the_ID(), 'hn_references', true );
    $refs     = $refs_raw ? json_decode( $refs_raw, true ) : [];
    if ( ! empty( $refs ) ) :
    ?>
    <div class="references">
      <h4>References</h4>
      <ol>
        <?php foreach ( $refs as $ref ) : ?>
        <li><?php echo wp_kses_post( $ref ); ?></li>
        <?php endforeach; ?>
      </ol>
    </div>
    <?php endif; ?>

    <!-- Inline Newsletter CTA -->
    <div class="inline-newsletter-cta">
      <div class="inline-cta-text">
        <h4>Weekly Health Insights</h4>
        <p>One evidence-based health insight every Wednesday. Free.</p>
      </div>
      <form class="inline-cta-form" onsubmit="hnNewsletterSubmit(event)">
        <input type="email" placeholder="your@email.com" required aria-label="Email" />
        <button type="submit">Subscribe</button>
      </form>
    </div>

    <!-- Related Articles -->
    <?php
    $related = new WP_Query( [
      'posts_per_page'      => 3,
      'post__not_in'        => [ get_the_ID() ],
      'category__in'        => wp_get_post_categories( get_the_ID() ),
      'post_status'         => 'publish',
      'orderby'             => 'rand',
      'ignore_sticky_posts' => 1,
    ] );
    if ( $related->have_posts() ) :
    ?>
    <div class="related-articles">
      <h3>Related Articles</h3>
      <div class="related-grid">
        <?php
        while ( $related->have_posts() ) : $related->the_post();
          echo hn_article_card( get_the_ID() );
        endwhile;
        wp_reset_postdata();
        ?>
      </div>
    </div>
    <?php endif; ?>

  </main>

  <!-- Sidebar -->
  <aside class="sidebar" role="complementary" aria-label="Sidebar">
    <!-- Table of Contents (JS-generated) -->
    <div class="sidebar-widget" id="toc-widget">
      <h4>Table of Contents</h4>
      <nav id="toc-nav" aria-label="Article sections"></nav>
    </div>

    <!-- Newsletter -->
    <div class="sidebar-widget">
      <h4>Weekly Insights</h4>
      <p style="font-size:13px;color:var(--ink-soft);margin-bottom:12px">Science-backed health content every Wednesday.</p>
      <form onsubmit="hnNewsletterSubmit(event)">
        <input type="email" placeholder="your@email.com" required
               style="width:100%;padding:10px 14px;border:1.5px solid var(--gray-200);border-radius:50px;font-size:13px;outline:none;margin-bottom:8px" />
        <button type="submit" class="sidebar-newsletter-btn">Subscribe Free</button>
      </form>
    </div>

    <!-- Category links -->
    <div class="sidebar-widget">
      <h4>Browse Categories</h4>
      <ul style="display:flex;flex-direction:column;gap:10px">
        <?php
        $sidebar_cats = get_categories( [ 'number' => 8, 'hide_empty' => true ] );
        foreach ( $sidebar_cats as $sc ) :
        ?>
        <li>
          <a href="<?php echo get_category_link( $sc->term_id ); ?>"
             style="font-size:13.5px;color:var(--ink-mid);display:flex;justify-content:space-between">
            <?php echo esc_html( $sc->name ); ?>
            <span style="color:var(--ink-faint)"><?php echo $sc->count; ?></span>
          </a>
        </li>
        <?php endforeach; ?>
      </ul>
    </div>
  </aside>
</div>

<?php endwhile; ?>
<?php get_footer(); ?>
