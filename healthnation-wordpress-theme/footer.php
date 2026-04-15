<footer class="site-footer" role="contentinfo">
  <div class="container">
    <div class="footer-top">

      <div class="footer-brand">
        <div class="footer-brand-logo">
          <div class="logo-mark" aria-hidden="true">🌿</div>
          <?php bloginfo( 'name' ); ?>
        </div>
        <p><?php bloginfo( 'description' ) ?: 'Science-backed health guidance reviewed by doctors and researchers. Built for people who want real answers.'; ?></p>
        <p class="footer-disclaimer">This site is for informational purposes only. Content does not constitute medical advice. Always consult a qualified healthcare provider.</p>
      </div>

      <div class="footer-col">
        <h5>Categories</h5>
        <ul>
          <?php
          $footer_cats = get_categories( [ 'number' => 6, 'hide_empty' => false ] );
          foreach ( $footer_cats as $fc ) :
          ?>
          <li><a href="<?php echo esc_url( get_category_link( $fc->term_id ) ); ?>"><?php echo esc_html( $fc->name ); ?></a></li>
          <?php endforeach; ?>
        </ul>
      </div>

      <div class="footer-col">
        <h5>Top Topics</h5>
        <ul>
          <?php
          $popular_tags = [ 'Gut Health', 'Weight Loss', 'Zone 2 Training', 'Sleep Science', 'Supplements', 'Blood Sugar' ];
          foreach ( $popular_tags as $tag ) :
            $t = get_term_by( 'name', $tag, 'post_tag' );
          ?>
          <li>
            <a href="<?php echo $t ? get_tag_link( $t->term_id ) : home_url( '/?s=' . urlencode( $tag ) ); ?>">
              <?php echo esc_html( $tag ); ?>
            </a>
          </li>
          <?php endforeach; ?>
        </ul>
      </div>

      <div class="footer-col">
        <h5>Tools</h5>
        <ul>
          <?php
          $tool_pages = [ 'BMR Calculator', 'Protein Calculator', 'Sleep Tracker', 'VO2 Max Estimator', 'Macros Calculator' ];
          foreach ( $tool_pages as $tp ) :
            $page = get_page_by_title( $tp );
          ?>
          <li>
            <a href="<?php echo $page ? get_permalink( $page->ID ) : home_url( '/tools/' . sanitize_title( $tp ) . '/' ); ?>">
              <?php echo esc_html( $tp ); ?>
            </a>
          </li>
          <?php endforeach; ?>
        </ul>
      </div>

      <div class="footer-col">
        <h5>Company</h5>
        <ul>
          <li><a href="<?php echo home_url( '/about/' ); ?>">About</a></li>
          <li><a href="<?php echo home_url( '/editorial-standards/' ); ?>">Editorial Standards</a></li>
          <li><a href="<?php echo home_url( '/medical-review-panel/' ); ?>">Medical Review Panel</a></li>
          <li><a href="<?php echo home_url( '/advertise/' ); ?>">Advertise</a></li>
          <li><a href="<?php echo home_url( '/contact/' ); ?>">Contact</a></li>
          <li><a href="<?php echo get_privacy_policy_url(); ?>">Privacy</a></li>
        </ul>
      </div>

    </div>

    <div class="footer-bottom">
      <p class="footer-copy">&copy; <?php echo date( 'Y' ); ?> <?php bloginfo( 'name' ); ?>. All rights reserved.</p>
      <div class="footer-legal-links">
        <a href="<?php echo get_privacy_policy_url(); ?>">Privacy Policy</a>
        <a href="<?php echo home_url( '/terms/' ); ?>">Terms of Use</a>
        <a href="<?php echo home_url( '/editorial-standards/' ); ?>">Editorial Standards</a>
        <a href="<?php echo home_url( '/medical-disclaimer/' ); ?>">Medical Disclaimer</a>
      </div>
    </div>
  </div>
</footer>

<!-- Mobile nav overlay styles (inline for performance) -->
<style>
.mobile-nav-overlay {
  position: fixed; inset: 0; z-index: 200; background: rgba(0,0,0,.5);
  display: none; align-items: flex-start; justify-content: flex-end;
}
.mobile-nav-overlay.open { display: flex; }
.mobile-nav-drawer {
  background: white; width: 85%; max-width: 320px; height: 100%;
  padding: 28px 24px; overflow-y: auto;
  box-shadow: -4px 0 24px rgba(0,0,0,.15);
}
.mobile-nav-close {
  background: none; border: none; font-size: 20px; color: var(--ink-soft);
  cursor: pointer; display: block; margin-left: auto; margin-bottom: 24px;
}
.mobile-menu-list { display: flex; flex-direction: column; gap: 4px; }
.mobile-menu-list li a {
  display: block; padding: 12px 14px; border-radius: var(--radius);
  font-size: 15px; font-weight: 500; color: var(--ink-mid); transition: background .15s;
}
.mobile-menu-list li a:hover { background: var(--gray-100); color: var(--ink); }
.search-modal {
  display: none; position: fixed; inset: 0; z-index: 300;
  background: rgba(0,0,0,.6); align-items: flex-start;
  justify-content: center; padding-top: 80px;
}
.search-modal.active { display: flex; }
.search-modal-inner {
  width: 90%; max-width: 640px; position: relative;
}
.search-modal-inner form {
  display: flex; background: white; border-radius: 50px;
  overflow: hidden; box-shadow: var(--shadow-lg);
}
.search-modal-inner input {
  flex: 1; border: none; outline: none; padding: 16px 24px;
  font-size: 16px; font-family: var(--sans); color: var(--ink);
}
.search-modal-inner button[type="submit"] {
  padding: 12px 24px; background: var(--sage); color: white;
  border: none; border-radius: 44px; margin: 5px;
  font-size: 14px; font-weight: 600; transition: background .15s;
}
.search-modal-inner button[type="submit"]:hover { background: #3d6b4a; }
.search-modal-close {
  position: absolute; top: -48px; right: 0;
  background: none; border: none; font-size: 22px; color: white; cursor: pointer;
}
.breadcrumbs { padding: calc(var(--nav-h) + 16px) 0 0; font-size: 12.5px; color: var(--ink-faint); }
.breadcrumbs ol { display: flex; flex-wrap: wrap; gap: 6px; list-style: none; }
.breadcrumbs li::after { content: '›'; margin-left: 6px; }
.breadcrumbs li:last-child::after { content: ''; }
.breadcrumbs a { color: var(--sage); }
.featured-wrap {
  border-radius: var(--radius-lg); overflow: hidden; background: var(--white);
  box-shadow: var(--shadow-md); display: grid; grid-template-columns: 1.2fr 1fr;
}
.feat-img { height: 420px; overflow: hidden; }
.feat-img img { width: 100%; height: 100%; object-fit: cover; }
.feat-body { padding: 44px; display: flex; flex-direction: column; justify-content: center; gap: 16px; }
.feat-label { display: inline-flex; align-items: center; gap: 8px; font-size: 11px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--sage); }
.feat-label-icon { width: 6px; height: 6px; border-radius: 50%; background: var(--sage); }
.feat-body h2 { font-family: var(--serif); font-size: clamp(20px, 2.5vw, 28px); color: var(--ink); line-height: 1.25; }
.feat-body h2 a:hover { color: var(--sage); }
.feat-body p { font-size: 14.5px; color: var(--ink-soft); line-height: 1.65; }
.feat-reviewer { display: flex; align-items: center; gap: 10px; padding: 12px 16px; background: var(--gray-50); border-radius: var(--radius); border-left: 3px solid var(--sage); }
.reviewer-avatar { width: 36px; height: 36px; border-radius: 50%; background: var(--sage-pale); display: flex; align-items: center; justify-content: center; font-size: 16px; flex-shrink: 0; }
.reviewer-info { font-size: 12px; }
.reviewer-info strong { display: block; color: var(--ink); font-weight: 600; }
.reviewer-info span { color: var(--ink-soft); }
.feat-meta { display: flex; gap: 10px; flex-wrap: wrap; }
.meta-pill { padding: 4px 12px; border-radius: 50px; font-size: 11.5px; font-weight: 500; background: var(--gray-100); color: var(--ink-mid); }
.feat-read-btn { display: inline-flex; align-items: center; gap: 8px; padding: 13px 24px; background: var(--sage); color: white; border-radius: 50px; font-size: 14px; font-weight: 600; transition: background .15s; align-self: flex-start; }
.feat-read-btn:hover { background: #3d6b4a; color: white; }
@media (max-width: 768px) {
  .featured-wrap { grid-template-columns: 1fr; }
  .feat-img { height: 260px; }
  .feat-body { padding: 28px 24px; }
}
</style>

<?php wp_footer(); ?>
</body>
</html>
