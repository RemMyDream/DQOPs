"""
Template Engine: Jinja2 SQL Renderer
Renders DQ check templates with context variables matching dialect/spark.sql.jinja2
"""
from typing import Dict, Any, Optional, List
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, TemplateNotFound
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TemplateEngine:
    """
    Jinja2-based SQL template engine for DQ checks.
    Works with dialect/spark.sql.jinja2 macros.
    """
    
    def __init__(self):
        """
        Initialize template engine
        """
        current_dir = Path(__file__).parent.parent
        template_dir = current_dir / "templates/sensors"
        
        self.template_dir = Path(template_dir)
        
        if not self.template_dir.exists():
            raise ValueError(f"Template directory not found: {self.template_dir}")
        
        # Initialize Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(self.template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=False,
            extensions=['jinja2.ext.do'] 
        )
                
        logger.info(f"Template engine initialized: {self.template_dir}")
    
    
    def render(self, template_name: str, context: Dict[str, Any]) -> str:
        """
        Render a template with given context
        
        Args:
            template_name: Template path relative to template_dir
            context: Context variables for the template
            
        Returns:
            Rendered SQL string
        """
        try:
            template = self.env.get_template(template_name)
            rendered = template.render(**context)
            
            # Clean up whitespace
            lines = [line.rstrip() for line in rendered.split('\n')]
            rendered = '\n'.join(lines)
            rendered = rendered.strip()
            
            # Remove multiple blank lines
            while '\n\n\n' in rendered:
                rendered = rendered.replace('\n\n\n', '\n\n')
            
            return rendered
            
        except TemplateNotFound as e:
            logger.error(f"Template not found: {template_name}")
            raise ValueError(f"Template not found: {template_name}") from e
        except Exception as e:
            logger.error(f"Error rendering template {template_name}: {e}")
            raise
    
    def render_from_check_definition(self, check_def) -> str:
        """
        Render sensor SQL from a CheckDefinition object
        
        Args:
            check_def: CheckDefinition object
            
        Returns:
            Rendered SQL string
        """
        template_path = check_def.sensor.get_template_path()
        context = check_def.build_template_context()
        
        return self.render(template_path, context)
    
    def render_error_sampler(
        self,
        target_table: Dict[str, str],
        table: Any,
        column_name: str,
        error_sampling: Dict[str, Any],
        error_condition: str,
        parameters: Any = None,
        data_groupings: Optional[Dict[str, Any]] = None,
        time_series: Any = None,
        time_window_filter: Any = None,
        additional_filters: Optional[List[str]] = None,
        wrap_condition: str = '',
        render_null_check: bool = True,
        override_samples_limit: Optional[int] = None,
        value_order_by: str = 'ASC'
    ) -> str:
        """
        Render error sampler query to get sample error rows
        
        This uses the render_error_sampler macro from the dialect
        """
        # Build context
        context = {
            'target_table': target_table,
            'table': table,
            'column_name': column_name,
            'error_sampling': error_sampling,
            'parameters': parameters or {},
            'additional_filters': additional_filters or [],
            'error_condition': error_condition,
            'wrap_condition': wrap_condition,
            'render_null_check': render_null_check,
            'override_samples_limit': override_samples_limit,
            'value_order_by': value_order_by,
        }
        
        if data_groupings:
            context['data_groupings'] = data_groupings
        
        if time_series:
            context['time_series'] = time_series
        
        if time_window_filter:
            context['time_window_filter'] = time_window_filter
        
        # Use error sampler template
        return self.render('error_sampler.sql.jinja2', context)
    
    def list_templates(self, category: Optional[str] = None) -> List[str]:
        """List available templates"""
        templates = []
        search_dir = self.template_dir
        
        if category:
            search_dir = search_dir / category
        
        if not search_dir.exists():
            return templates
        
        for path in search_dir.rglob("*.jinja2"):
            rel_path = path.relative_to(self.template_dir)
            templates.append(str(rel_path))
        
        return sorted(templates)
    
    def list_sensors(self) -> Dict[str, List[str]]:
        """List available sensors grouped by category"""
        templates = self.list_templates('checks')
        
        sensors_by_category = {}
        for template in templates:
            parts = template.replace('.sql.jinja2', '').split('/')
            if len(parts) >= 3:
                category = parts[1]
                sensor_type = parts[2]
                
                if category not in sensors_by_category:
                    sensors_by_category[category] = []
                sensors_by_category[category].append(sensor_type)
        
        return sensors_by_category


# Singleton instance
_engine: Optional[TemplateEngine] = None


def get_template_engine(template_dir: str = None) -> TemplateEngine:
    """Get or create template engine singleton"""
    global _engine
    if _engine is None:
        _engine = TemplateEngine()
    return _engine
