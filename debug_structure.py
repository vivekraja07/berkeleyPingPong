import requests
import re
from bs4 import BeautifulSoup

url = 'https://berkeleytabletennis.org/results/rr_results_2025nov07'
page = requests.get(url)
soup = BeautifulSoup(page.content, 'html.parser')

# Find first group header
header = soup.find(string=re.compile(r'^#1$')).parent
print('Header:', header)
print('Header classes:', header.get('class'))
print('Next sibling:', header.find_next_sibling())
print('Next element:', header.find_next())

# Find the table structure
current = header.find_next()
print('\n=== Examining structure after header ===')
for i in range(5):
    if not current:
        break
    print(f'\nElement {i+1}: {current.name}')
    print(f'  Classes: {current.get("class")}')
    print(f'  Text preview: {current.get_text(strip=True)[:50]}')
    if current.name == 'div':
        children = list(current.children)
        div_children = [c for c in children if hasattr(c, 'name') and c.name == 'div']
        print(f'  Direct div children: {len(div_children)}')
        if div_children:
            print(f'  First child classes: {div_children[0].get("class") if div_children else None}')
            print(f'  First child text: {div_children[0].get_text(strip=True)[:30] if div_children else None}')
    current = current.find_next()

