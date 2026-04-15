<?php get_header(); ?>

<!-- ╔═══════════ HERO ═══════════╗ -->
<section class="hero-section">
  <div class="hero-blob" aria-hidden="true"></div>
  <div class="hero-inner">
    <div class="hero-text">
      <div class="hero-overline">
        <span class="hero-overline-dot" aria-hidden="true"></span>
        Evidence-based health guidance for real people
      </div>
      <h1 class="hero-h1">Your Health, Explained<br><em>Without the Noise</em></h1>
      <p class="hero-sub">Science-backed content on nutrition, fitness, mental health, and longevity — reviewed by doctors and researchers, written for everyday life.</p>

      <form class="hero-search-form" role="search" method="get" action="<?php echo home_url( '/' ); ?>">
        <input type="search" name="s" placeholder="Search symptoms, conditions, or health topics…"
               value="<?php echo get_search_query(); ?>" aria-label="Search HealthNation" />
        <button type="submit">Search</button>
      </form>

      <div class="quick-topics" role="list" aria-label="Popular topics">
        <?php
        $quick_topics = [
          'gut-health'    => '🦠 Gut Health',
          'sleep'         => '😴 Sleep',
          'weight-loss'   => '⚖️ Weight Loss',
          'stress'        => '🧠 Stress',
          'longevity'     => '⏳ Longevity',
          'blood-sugar'   => '🩸 Blood Sugar',
        ];
        foreach ( $quick_topics as $slug => $label ) {
          $cat = get_category_by_slug( $slug );
          $url = $cat ? get_category_link( $cat->term_id ) : home_url( '/?s=' . $slug );
          echo '<a href="' . esc_url( $url ) . '" class="quick-tag" role="listitem">' . esc_html( $label ) . '</a>';
        }
        ?>
      </div>
    </div>

    <div class="hero-visual" aria-hidden="true">
      <div class="hero-main-img">
        <img src="https://images.unsplash.com/photo-1490645935967-10de6ba17061?w=800&q=80&fit=crop"
             alt="Healthy food preparation on a bright kitchen counter" width="800" height="480" />
      </div>
      <div class="hero-badge">
        <div class="badge-icon">🩺</div>
        <div>
          <div class="badge-text">Medically Reviewed</div>
          <div class="badge-sub">Every article reviewed by a licensed physician</div>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- ╔═══════════ TRUST BAR ═══════════╗ -->
<div class="trust-bar" role="list" aria-label="Site trust signals">
  <div class="trust-item" role="listitem"><div class="trust-dot" aria-hidden="true"></div>Reviewed by licensed physicians</div>
  <div class="trust-item" role="listitem"><div class="trust-dot" aria-hidden="true"></div>Cites peer-reviewed research</div>
  <div class="trust-item" role="listitem"><div class="trust-dot" aria-hidden="true"></div>No affiliate bias</div>
  <div class="trust-item" role="listitem"><div class="trust-dot" aria-hidden="true"></div>1.8M monthly readers</div>
</div>

<!-- ╔═══════════ CATEGORY GRID ═══════════╗ -->
<section class="section categories-section">
  <div class="container">
    <div class="section-header" style="margin-bottom:40px">
      <p class="section-overline">Browse by goal</p>
      <h2 class="section-title">Explore by Health Goal</h2>
      <p class="section-sub">Every category built on the same principle: the evidence first, practical application second.</p>
    </div>
    <div class="cat-grid">
      <?php
      $main_cats = [
        [ 'slug' => 'nutrition',     'icon' => '🥗', 'h3' => 'Nutrition Science for Real People',     'desc' => 'Evidence-based guides covering macros, gut health, meal planning, diets, and supplements.' ],
        [ 'slug' => 'fitness',       'icon' => '🏋️', 'h3' => 'Train Smarter. Recover Better.',         'desc' => 'Strength training, Zone 2 cardio, VO2 max, recovery science, and sport-specific performance.' ],
        [ 'slug' => 'mental-health', 'icon' => '🧠', 'h3' => 'Mental Health, Backed by Neuroscience',  'desc' => 'Stress, anxiety, burnout, sleep, mindfulness — written with clinical accuracy.' ],
        [ 'slug' => 'longevity',     'icon' => '⏳', 'h3' => 'The Science of Living Longer',           'desc' => 'Blue Zones, VO2 max, muscle preservation, NAD+, fasting, and longevity biomarkers.' ],
        [ 'slug' => 'sleep',         'icon' => '😴', 'h3' => 'Sleep Is Your Most Important Variable',   'desc' => 'Sleep architecture, circadian rhythm, sleep disorders, and evidence-based optimization.' ],
        [ 'slug' => 'conditions',    'icon' => '🩺', 'h3' => 'Your Conditions, In Plain English',       'desc' => 'Condition-specific guides covering causes, evidence-based treatment, and lifestyle interventions.' ],
      ];

      foreach ( $main_cats as $cat_data ) :
        $cat   = get_category_by_slug( $cat_data['slug'] );
        $count = $cat ? $cat->count : 0;
        $url   = $cat ? get_category_link( $cat->term_id ) : '#';
        $img   = $cat ? hn_get_category_image_url( $cat->term_id ) : '';
      ?>
      <article class="cat-card">
        <?php if ( $img ) : ?>
        <a href="<?php echo esc_url( $url ); ?>" class="cat-img" tabindex="-1" aria-hidden="true">
          <img src="<?php echo esc_url( $img ); ?>" alt="<?php echo esc_attr( $cat_data['h3'] ); ?>" loading="lazy" />
        </a>
        <?php endif; ?>
        <div class="cat-body">
          <div class="cat-tag"><span class="cat-tag-dot" aria-hidden="true"></span><?php echo esc_html( ucfirst( $cat_data['slug'] ) ); ?></div>
          <h3><a href="<?php echo esc_url( $url ); ?>"><?php echo esc_html( $cat_data['h3'] ); ?></a></h3>
          <p><?php echo esc_html( $cat_data['desc'] ); ?></p>
        </div>
        <div class="cat-footer">
          <span class="cat-count"><?php echo $count > 0 ? $count . ' articles' : 'Coming soon'; ?></span>
          <a href="<?php echo esc_url( $url ); ?>" class="cat-arrow" aria-label="View <?php echo esc_attr( $cat_data['slug'] ); ?> articles">→</a>
        </div>
      </article>
      <?php endforeach; ?>
    </div>
  </div>
</section>

<!-- ╔═══════════ FEATURED ARTICLE ═══════════╗ -->
<?php
$featured_id = get_option( 'hn_featured_post_id' );
if ( ! $featured_id ) {
  $featured_query = new WP_Query( [ 'posts_per_page' => 1, 'post_status' => 'publish', 'orderby' => 'date', 'order' => 'DESC' ] );
  $featured_id = $featured_query->posts[0]->ID ?? 0;
}
if ( $featured_id ) :
  $feat_post = get_post( $featured_id );
  $feat_img  = get_the_post_thumbnail_url( $featured_id, 'hn-hero' );
  $feat_cat  = get_the_category( $featured_id );
  $feat_read = hn_reading_time( $featured_id );
  $reviewer  = get_post_meta( $featured_id, 'hn_reviewer_name', true );
  $reviewer_creds = get_post_meta( $featured_id, 'hn_reviewer_credentials', true );
  $reviewer_spec  = get_post_meta( $featured_id, 'hn_reviewer_specialty', true );
  $citations = get_post_meta( $featured_id, 'hn_citation_count', true );
?>
<section class="section" style="background:var(--white)">
  <div class="container">
    <div style="margin-bottom:40px">
      <p class="section-overline">This week's deep dive</p>
      <h2 class="section-title">Editor's Pick</h2>
    </div>
    <div class="featured-wrap">
      <?php if ( $feat_img ) : ?>
      <a href="<?php echo get_permalink( $featured_id ); ?>" class="feat-img" tabindex="-1">
        <img src="<?php echo esc_url( $feat_img ); ?>" alt="<?php echo esc_attr( get_the_title( $featured_id ) ); ?>" />
      </a>
      <?php endif; ?>
      <div class="feat-body">
        <?php if ( $feat_cat ) : ?>
        <a href="<?php echo get_category_link( $feat_cat[0]->term_id ); ?>" class="feat-label">
          <span class="feat-label-icon" aria-hidden="true"></span>
          <?php echo esc_html( $feat_cat[0]->name ); ?>
        </a>
        <?php endif; ?>
        <h2><a href="<?php echo get_permalink( $featured_id ); ?>"><?php echo get_the_title( $featured_id ); ?></a></h2>
        <p><?php echo wp_trim_words( get_the_excerpt( $featured_id ), 30 ); ?></p>
        <?php if ( $reviewer ) : ?>
        <div class="feat-reviewer">
          <div class="reviewer-avatar">👨‍⚕️</div>
          <div class="reviewer-info">
            <strong>Reviewed by <?php echo esc_html( $reviewer ); ?><?php if ($reviewer_creds) echo ', ' . esc_html($reviewer_creds); ?></strong>
            <span><?php echo esc_html( $reviewer_spec ?: 'Medical Reviewer' ); ?></span>
          </div>
        </div>
        <?php endif; ?>
        <div class="feat-meta">
          <span class="meta-pill"><?php echo $feat_read; ?> min read</span>
          <span class="meta-pill"><?php echo get_the_date( 'M Y', $featured_id ); ?></span>
          <?php if ( $citations ) : ?><span class="meta-pill"><?php echo $citations; ?> citations</span><?php endif; ?>
        </div>
        <a href="<?php echo get_permalink( $featured_id ); ?>" class="feat-read-btn">Read the Guide →</a>
      </div>
    </div>
  </div>
</section>
<?php endif; ?>

<!-- ╔═══════════ LATEST ARTICLES ═══════════╗ -->
<section class="section articles-section">
  <div class="container">
    <div style="margin-bottom:32px">
      <h2 class="section-title">Latest From HealthNation</h2>
      <p class="section-sub">New articles published daily, each reviewed before going live.</p>
    </div>
    <div class="article-filters" role="tablist" aria-label="Filter articles by category">
      <button class="art-filter active" data-cat="all" role="tab" aria-selected="true">All</button>
      <?php
      $filter_cats = get_categories( [ 'number' => 6, 'hide_empty' => true ] );
      foreach ( $filter_cats as $fc ) {
        echo '<button class="art-filter" data-cat="' . esc_attr( $fc->slug ) . '" role="tab" aria-selected="false">' . esc_html( $fc->name ) . '</button>';
      }
      ?>
    </div>
    <div class="articles-grid" id="articles-grid">
      <?php
      $recent = new WP_Query( [
        'posts_per_page' => 6,
        'post_status'    => 'publish',
        'orderby'        => 'date',
        'order'          => 'DESC',
      ] );
      if ( $recent->have_posts() ) :
        while ( $recent->have_posts() ) : $recent->the_post();
          echo hn_article_card( get_the_ID() );
        endwhile;
        wp_reset_postdata();
      endif;
      ?>
    </div>
    <div style="text-align:center;margin-top:40px">
      <a href="<?php echo get_permalink( get_option( 'page_for_posts' ) ) ?: home_url( '/blog' ); ?>"
         class="nav-cta" style="display:inline-block;padding:14px 32px;font-size:15px">
        Load More Articles →
      </a>
    </div>
  </div>
</section>

<!-- ╔═══════════ LONGEVITY SPOTLIGHT ═══════════╗ -->
<section class="longevity-section">
  <div class="container">
    <p class="section-overline">Live longer. Live better.</p>
    <h2 class="section-title">What the Longest-Lived People Do <em>Differently</em></h2>
    <p class="section-sub">We break down Blue Zones research, the latest longevity science, and what it means for your daily routine.</p>
    <div class="longevity-grid">
      <?php
      $longevity_cat = get_category_by_slug( 'longevity' );
      $long_posts = new WP_Query( [
        'posts_per_page' => 3,
        'cat'            => $longevity_cat ? $longevity_cat->term_id : 0,
        'post_status'    => 'publish',
        'orderby'        => 'date',
        'order'          => 'DESC',
      ] );
      $n = 1;
      if ( $long_posts->have_posts() ) :
        while ( $long_posts->have_posts() ) : $long_posts->the_post();
      ?>
      <article class="longevity-card">
        <div class="longevity-num" aria-hidden="true"><?php echo str_pad( $n, 2, '0', STR_PAD_LEFT ); ?></div>
        <h3><a href="<?php the_permalink(); ?>" style="color:white"><?php the_title(); ?></a></h3>
        <p><?php echo wp_trim_words( get_the_excerpt(), 20 ); ?></p>
      </article>
      <?php $n++; endwhile; wp_reset_postdata();
      else :
        $placeholders = [
          [ '01', 'Blue Zones: The 9 Habits of the World\'s Longest-Lived People', 'What Okinawa, Sardinia, and Loma Linda all have in common.' ],
          [ '02', 'Why VO2 Max May Be the Best Predictor of Longevity',            'The data behind cardiorespiratory fitness and mortality risk.' ],
          [ '03', 'Muscle Mass After 40: A Longevity Non-Negotiable',              'Sarcopenia, metabolic health, and resistance training as medicine.' ],
        ];
        foreach ( $placeholders as $p ) :
      ?>
      <div class="longevity-card">
        <div class="longevity-num" aria-hidden="true"><?php echo $p[0]; ?></div>
        <h3><?php echo esc_html( $p[1] ); ?></h3>
        <p><?php echo esc_html( $p[2] ); ?></p>
      </div>
      <?php endforeach; endif; ?>
    </div>
    <?php if ( $longevity_cat ) : ?>
    <a href="<?php echo get_category_link( $longevity_cat->term_id ); ?>" class="longevity-cta">Explore Longevity Science →</a>
    <?php endif; ?>
  </div>
</section>

<!-- ╔═══════════ TOOLS ═══════════╗ -->
<section class="section" style="background:var(--white)">
  <div class="container">
    <div style="margin-bottom:40px">
      <p class="section-overline">Free to use</p>
      <h2 class="section-title">Health Calculators &amp; Tools</h2>
      <p class="section-sub">Evidence-based calculators to help you track the numbers that matter.</p>
    </div>
    <div class="tools-grid">
      <?php
      $tools = [
        [ '🔥', 'BMR Calculator',       'Your base metabolic rate',     '/tools/bmr-calculator/' ],
        [ '🥩', 'Protein Calculator',   'Daily intake by goal & weight', '/tools/protein-calculator/' ],
        [ '😴', 'Sleep Debt Tracker',   'How much sleep you owe',        '/tools/sleep-tracker/' ],
        [ '🫁', 'VO2 Max Estimator',    'Estimate from resting HR',      '/tools/vo2-max-estimator/' ],
        [ '💧', 'Hydration Calculator', 'Daily water needs by activity', '/tools/hydration-calculator/' ],
        [ '📊', 'Macros Calculator',    'Protein, carbs, fat by goal',   '/tools/macros-calculator/' ],
      ];
      foreach ( $tools as $tool ) : ?>
      <a href="<?php echo home_url( $tool[3] ); ?>" class="tool-card">
        <div class="tool-icon" aria-hidden="true"><?php echo $tool[0]; ?></div>
        <div class="tool-text">
          <h4><?php echo esc_html( $tool[1] ); ?></h4>
          <p><?php echo esc_html( $tool[2] ); ?></p>
        </div>
      </a>
      <?php endforeach; ?>
    </div>
  </div>
</section>

<!-- ╔═══════════ NEWSLETTER ═══════════╗ -->
<section class="newsletter-section" id="newsletter">
  <div class="newsletter-inner">
    <p class="section-overline">Every Wednesday</p>
    <h2 class="section-title">Weekly Health Insights.<br>No Fads. No Noise.</h2>
    <p class="section-sub">One evidence-based health insight every Wednesday — new research, practical tips, and myth-busting. Free, always.</p>
    <form class="newsletter-form" id="newsletter-form" onsubmit="hnNewsletterSubmit(event)">
      <input type="email" name="email" placeholder="your@email.com" required aria-label="Email address" />
      <button type="submit">Subscribe Free</button>
    </form>
    <p class="newsletter-disclaimer">No spam. Unsubscribe any time. We never sell your data.</p>
  </div>
</section>

<?php get_footer(); ?>
