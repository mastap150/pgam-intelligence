<?php get_header(); ?>

<main style="padding-top:var(--nav-h)">
  <div class="container" style="padding-top:48px;padding-bottom:80px">
    <h1 class="section-title" style="margin-bottom:32px">
      <?php
      if ( is_search() ) echo 'Search Results for: ' . get_search_query();
      elseif ( is_archive() ) the_archive_title();
      else echo 'Latest Articles';
      ?>
    </h1>

    <?php if ( is_category() ) : ?>
    <p class="section-sub" style="margin-bottom:40px"><?php echo category_description(); ?></p>
    <?php endif; ?>

    <div class="articles-grid">
      <?php if ( have_posts() ) :
        while ( have_posts() ) : the_post();
          echo hn_article_card( get_the_ID() );
        endwhile;
      else : ?>
        <p style="color:var(--ink-soft);font-size:16px;grid-column:1/-1">
          No articles found.
          <?php if ( is_search() ) echo 'Try a different search term.'; ?>
        </p>
      <?php endif; ?>
    </div>

    <div style="margin-top:48px">
      <?php the_posts_pagination( [
        'mid_size'  => 2,
        'prev_text' => '← Previous',
        'next_text' => 'Next →',
      ] ); ?>
    </div>
  </div>
</main>

<?php get_footer(); ?>
