document.addEventListener('DOMContentLoaded', function () {
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.querySelector('.main-content');
    const toggleBtn = document.querySelector('#sidebar-toggle');

    // Check localStorage for sidebar state
    if (localStorage.getItem('sidebarCollapsed') === 'true') {
        sidebar.classList.add('collapsed');
        mainContent.classList.add('expanded');
    }

    toggleBtn.addEventListener('click', function () {
        sidebar.classList.toggle('collapsed');
        mainContent.classList.toggle('expanded');
        // Save state to localStorage
        localStorage.setItem('sidebarCollapsed', sidebar.classList.contains('collapsed'));
    });

    var elems = document.querySelectorAll('.submenu-trigger');
    elems.forEach(function(elem) {
        elem.addEventListener('click', function(event) {
            if (!sidebar.classList.contains('collapsed')) {
                event.preventDefault();
                this.classList.toggle('active');
            }
        });
    });

    // Smart table scroll detection
    function checkTableScroll() {
        const tables = document.querySelectorAll('.responsive-table');
        tables.forEach(function(table) {
            const tableElement = table.querySelector('table');
            if (tableElement) {
                // Check if table content is wider than container
                const tableWidth = tableElement.scrollWidth;
                const containerWidth = table.offsetWidth;
                
                if (tableWidth > containerWidth) {
                    // Table needs horizontal scroll
                    table.classList.add('force-scroll');
                    table.style.overflowX = 'auto';
                } else {
                    // Table fits, no scroll needed
                    table.classList.remove('force-scroll');
                    table.style.overflowX = 'hidden';
                }
            }
        });
    }

    // Check tables on page load
    checkTableScroll();

    // Check tables when window resizes
    window.addEventListener('resize', checkTableScroll);

    // Check tables after any dynamic content changes (like processing sessions)
    const observer = new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
            if (mutation.type === 'childList' || mutation.type === 'attributes') {
                setTimeout(checkTableScroll, 100); // Small delay to ensure DOM is updated
            }
        });
    });

    // Observe the main content area for changes
    if (mainContent) {
        observer.observe(mainContent, {
            childList: true,
            subtree: true,
            attributes: true
        });
    }
});
